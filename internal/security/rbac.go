package security

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Permission represents a specific action that can be performed
type Permission string

const (
	PermissionSubmitJob   Permission = "job:submit"
	PermissionCancelJob   Permission = "job:cancel"
	PermissionViewJob     Permission = "job:view"
	PermissionListJobs    Permission = "job:list"
	PermissionManageNodes Permission = "cluster:manage"
	PermissionViewMetrics Permission = "metrics:view"
	PermissionAdmin       Permission = "admin:*"
)

// Role represents a user role with associated permissions
type Role struct {
	Name        string
	Permissions []Permission
}

var (
	// Predefined roles
	RoleAdmin = Role{
		Name: "admin",
		Permissions: []Permission{
			PermissionAdmin, // Admin has all permissions
		},
	}

	RoleOperator = Role{
		Name: "operator",
		Permissions: []Permission{
			PermissionSubmitJob,
			PermissionCancelJob,
			PermissionViewJob,
			PermissionListJobs,
			PermissionViewMetrics,
		},
	}

	RoleViewer = Role{
		Name: "viewer",
		Permissions: []Permission{
			PermissionViewJob,
			PermissionListJobs,
			PermissionViewMetrics,
		},
	}

	RoleWorker = Role{
		Name: "worker",
		Permissions: []Permission{
			PermissionViewJob,
			PermissionListJobs,
		},
	}
)

// User represents an authenticated user with roles
type User struct {
	ID       string
	Username string
	Roles    []Role
}

// HasPermission checks if user has a specific permission
func (u *User) HasPermission(perm Permission) bool {
	for _, role := range u.Roles {
		// Admin role has all permissions
		for _, p := range role.Permissions {
			if p == PermissionAdmin || p == perm {
				return true
			}
		}
	}
	return false
}

// RBAC implements role-based access control
type RBAC struct {
	mu       sync.RWMutex
	users    map[string]*User        // userID -> User
	roles    map[string]Role         // roleName -> Role
	policies map[string][]Permission // method -> required permissions
	logger   *zap.Logger
}

// NewRBAC creates a new RBAC instance
func NewRBAC(logger *zap.Logger) *RBAC {
	rbac := &RBAC{
		users:    make(map[string]*User),
		roles:    make(map[string]Role),
		policies: make(map[string][]Permission),
		logger:   logger,
	}

	// Register default roles
	rbac.RegisterRole(RoleAdmin)
	rbac.RegisterRole(RoleOperator)
	rbac.RegisterRole(RoleViewer)
	rbac.RegisterRole(RoleWorker)

	// Define method policies
	rbac.DefinePolicy("/scheduler.Scheduler/SubmitJob", []Permission{PermissionSubmitJob})
	rbac.DefinePolicy("/scheduler.Scheduler/CancelJob", []Permission{PermissionCancelJob})
	rbac.DefinePolicy("/scheduler.Scheduler/GetJobStatus", []Permission{PermissionViewJob})
	rbac.DefinePolicy("/scheduler.Scheduler/ListJobs", []Permission{PermissionListJobs})
	rbac.DefinePolicy("/scheduler.Scheduler/JoinCluster", []Permission{PermissionManageNodes})

	return rbac
}

// RegisterRole registers a new role
func (r *RBAC) RegisterRole(role Role) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.roles[role.Name] = role
	r.logger.Info("Registered role", zap.String("role", role.Name), zap.Int("permissions", len(role.Permissions)))
}

// AddUser adds a user with specified roles
func (r *RBAC) AddUser(userID, username string, roleNames ...string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var roles []Role
	for _, roleName := range roleNames {
		role, ok := r.roles[roleName]
		if !ok {
			return fmt.Errorf("role not found: %s", roleName)
		}
		roles = append(roles, role)
	}

	r.users[userID] = &User{
		ID:       userID,
		Username: username,
		Roles:    roles,
	}

	r.logger.Info("Added user", zap.String("user_id", userID), zap.String("username", username), zap.Strings("roles", roleNames))
	return nil
}

// GetUser retrieves a user by ID
func (r *RBAC) GetUser(userID string) (*User, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	user, ok := r.users[userID]
	if !ok {
		return nil, fmt.Errorf("user not found: %s", userID)
	}
	return user, nil
}

// DefinePolicy defines required permissions for a gRPC method
func (r *RBAC) DefinePolicy(method string, permissions []Permission) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.policies[method] = permissions
	r.logger.Debug("Defined policy", zap.String("method", method), zap.Int("permissions", len(permissions)))
}

// CheckPermission checks if a user has required permissions for a method
func (r *RBAC) CheckPermission(userID string, method string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Get user
	user, ok := r.users[userID]
	if !ok {
		return errors.New("user not found")
	}

	// Get required permissions for method
	requiredPerms, ok := r.policies[method]
	if !ok {
		// No policy defined - allow by default
		return nil
	}

	// Check if user has any of the required permissions
	for _, perm := range requiredPerms {
		if user.HasPermission(perm) {
			return nil
		}
	}

	return fmt.Errorf("user %s lacks required permissions for %s", userID, method)
}

// RBACInterceptor creates a gRPC interceptor for RBAC
func (r *RBAC) RBACInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// Skip RBAC for health checks
		if info.FullMethod == "/grpc.health.v1.Health/Check" {
			return handler(ctx, req)
		}

		// Extract user ID from context (set by auth interceptor)
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			r.logger.Warn("RBAC: Missing metadata", zap.String("method", info.FullMethod))
			// In development, allow requests without metadata
			// Production: return status.Error(codes.PermissionDenied, "missing metadata")
			return handler(ctx, req)
		}

		userIDs := md.Get("user-id")
		if len(userIDs) == 0 {
			r.logger.Warn("RBAC: Missing user ID", zap.String("method", info.FullMethod))
			// In development, allow requests without user ID
			// Production: return status.Error(codes.PermissionDenied, "missing user id")
			return handler(ctx, req)
		}

		userID := userIDs[0]

		// Check permissions
		if err := r.CheckPermission(userID, info.FullMethod); err != nil {
			r.logger.Warn("RBAC: Permission denied",
				zap.String("user_id", userID),
				zap.String("method", info.FullMethod),
				zap.Error(err),
			)
			return nil, status.Errorf(codes.PermissionDenied, "permission denied: %v", err)
		}

		r.logger.Debug("RBAC: Access granted",
			zap.String("user_id", userID),
			zap.String("method", info.FullMethod),
		)

		return handler(ctx, req)
	}
}

// RBACConfig holds RBAC configuration
type RBACConfig struct {
	Enabled bool
	// Add more configuration options as needed
}
