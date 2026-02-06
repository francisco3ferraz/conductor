package security

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Permission represents a specific action that can be performed
type Permission string

const (
	PermissionSubmitJob   Permission = "job:submit"
	PermissionCancelJob   Permission = "job:cancel"
	PermissionViewJob     Permission = "job:view"
	PermissionListJobs    Permission = "job:list"
	PermissionViewDLQ     Permission = "job:dlq:view"
	PermissionRetryDLQ    Permission = "job:dlq:retry"
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
			PermissionViewDLQ,
			PermissionRetryDLQ,
			PermissionViewMetrics,
		},
	}

	RoleViewer = Role{
		Name: "viewer",
		Permissions: []Permission{
			PermissionViewJob,
			PermissionListJobs,
			PermissionViewDLQ,
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
	mu            sync.RWMutex
	users         map[string]*User // userID -> User
	roles         map[string]Role  // roleName -> Role (for backward compat)
	policyManager *PolicyManager
	config        *RBACConfig
	logger        *zap.Logger
	auditLogger   *zap.Logger
}

// RBACConfig holds RBAC configuration
type RBACConfig struct {
	Enabled         bool
	DevelopmentMode bool   // Explicit dev mode flag
	PolicyFile      string // Path to policy configuration file
}

// NewRBAC creates a new RBAC instance
func NewRBAC(config *RBACConfig, logger *zap.Logger) *RBAC {
	policyManager := NewPolicyManager(logger)

	// Load policies from file if specified
	if config.PolicyFile != "" {
		if err := policyManager.LoadFromFile(config.PolicyFile); err != nil {
			logger.Warn("Failed to load policy file, using defaults",
				zap.String("file", config.PolicyFile),
				zap.Error(err))
			policyManager.LoadFromConfig(DefaultPolicyConfig())
		}
	} else {
		// Use default policies
		policyManager.LoadFromConfig(DefaultPolicyConfig())
	}

	rbac := &RBAC{
		users:         make(map[string]*User),
		roles:         make(map[string]Role),
		policyManager: policyManager,
		config:        config,
		logger:        logger,
		auditLogger:   logger.Named("rbac-audit"),
	}

	// Register default roles for backward compatibility
	rbac.RegisterRole(RoleAdmin)
	rbac.RegisterRole(RoleOperator)
	rbac.RegisterRole(RoleViewer)
	rbac.RegisterRole(RoleWorker)

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

// GetUserRoles returns role names for a user (implements RoleProvider interface)
func (r *RBAC) GetUserRoles(userID string) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	user, ok := r.users[userID]
	if !ok {
		return nil, fmt.Errorf("user not found: %s", userID)
	}

	roleNames := make([]string, len(user.Roles))
	for i, role := range user.Roles {
		roleNames[i] = role.Name
	}
	return roleNames, nil
}

// DefinePolicy defines required permissions for a gRPC method
func (r *RBAC) DefinePolicy(method string, permissions []Permission) {
	// Convert Permission constants to strings for PolicyManager
	permStrings := make([]string, len(permissions))
	for i, perm := range permissions {
		permStrings[i] = string(perm)
	}

	policy := PolicyDefinition{
		Method:      method,
		Permissions: permStrings,
		Description: fmt.Sprintf("Policy for %s", method),
	}

	r.policyManager.AddPolicy(policy)
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
	// Get required permissions from policy manager
	requiredPerms, ok := r.policyManager.GetMethodPermissions(method)
	if !ok {
		// No policy defined - deny by default for security
		// Allow only health checks and registration
		if method == "/grpc.health.v1.Health/Check" || method == "/proto.MasterService/RegisterWorker" {
			return nil
		}
		r.logger.Warn("No RBAC policy defined for method - denying access",
			zap.String("method", method),
			zap.String("user_id", userID))
		return fmt.Errorf("no RBAC policy defined for method %s", method)
	}

	// Check if user has any of the required permissions
	for _, permStr := range requiredPerms {
		// Convert string permission to Permission constant
		perm := Permission(permStr)
		if user.HasPermission(perm) {
			return nil
		}
	}

	return fmt.Errorf("user %s lacks required permissions for %s", userID, method)
}

// DefineDefaultPolicies defines default RBAC policies for all gRPC endpoints
func (r *RBAC) DefineDefaultPolicies() {
	// Job submission and management
	r.DefinePolicy("/proto.MasterService/SubmitJob", []Permission{PermissionSubmitJob})
	r.DefinePolicy("/proto.MasterService/CancelJob", []Permission{PermissionCancelJob})

	// Job viewing
	r.DefinePolicy("/proto.MasterService/GetJobStatus", []Permission{PermissionViewJob})
	r.DefinePolicy("/proto.MasterService/ListJobs", []Permission{PermissionListJobs})

	// Cluster management
	r.DefinePolicy("/proto.MasterService/JoinCluster", []Permission{PermissionManageNodes})
	r.DefinePolicy("/proto.MasterService/GetClusterHealth", []Permission{PermissionViewMetrics})
	r.DefinePolicy("/proto.MasterService/ListWorkers", []Permission{PermissionViewMetrics})

	// Worker operations (workers should have these permissions)
	r.DefinePolicy("/proto.MasterService/Heartbeat", []Permission{PermissionViewJob})    // Workers can heartbeat
	r.DefinePolicy("/proto.MasterService/ReportResult", []Permission{PermissionViewJob}) // Workers can report results

	// DLQ operations
	r.DefinePolicy("/proto.MasterService/ListDLQ", []Permission{PermissionViewDLQ})
	r.DefinePolicy("/proto.MasterService/RetryFromDLQ", []Permission{PermissionRetryDLQ})

	r.logger.Info("Default RBAC policies defined for all endpoints")
}

// RBACInterceptor creates a gRPC interceptor for RBAC
func (r *RBAC) RBACInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()

		// Extract client address for audit logging
		clientAddr := "unknown"
		if p, ok := peer.FromContext(ctx); ok {
			clientAddr = p.Addr.String()
		}

		// Skip RBAC for health checks
		if info.FullMethod == "/grpc.health.v1.Health/Check" {
			return handler(ctx, req)
		}

		// Check if RBAC is enabled
		if !r.config.Enabled {
			r.logger.Debug("RBAC disabled, allowing request")
			return handler(ctx, req)
		}

		// Extract user ID from context (set by auth interceptor)
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			r.auditLogger.Warn("Authorization failed: missing metadata",
				zap.String("method", info.FullMethod),
				zap.String("client", clientAddr),
				zap.String("reason", "no_metadata"),
			)

			if r.config.DevelopmentMode {
				r.logger.Warn("DEV MODE: Allowing request without metadata")
				return handler(ctx, req)
			}
			return nil, status.Error(codes.PermissionDenied, "missing metadata")
		}

		userIDs := md.Get("user-id")
		if len(userIDs) == 0 {
			r.auditLogger.Warn("Authorization failed: missing user ID",
				zap.String("method", info.FullMethod),
				zap.String("client", clientAddr),
				zap.String("reason", "no_user_id"),
			)

			if r.config.DevelopmentMode {
				r.logger.Warn("DEV MODE: Allowing request without user ID")
				return handler(ctx, req)
			}
			return nil, status.Error(codes.PermissionDenied, "missing user id")
		}

		userID := userIDs[0]

		// Check permissions
		if err := r.CheckPermission(userID, info.FullMethod); err != nil {
			r.auditLogger.Warn("Authorization failed: permission denied",
				zap.String("user_id", userID),
				zap.String("method", info.FullMethod),
				zap.String("client", clientAddr),
				zap.Error(err),
				zap.Duration("latency", time.Since(start)),
			)
			return nil, status.Errorf(codes.PermissionDenied, "permission denied")
		}

		r.auditLogger.Info("Authorization successful",
			zap.String("user_id", userID),
			zap.String("method", info.FullMethod),
			zap.String("client", clientAddr),
			zap.Duration("latency", time.Since(start)),
		)

		return handler(ctx, req)
	}
}
