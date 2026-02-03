package security

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"go.uber.org/zap"
)

// PolicyConfig represents configurable RBAC policies
type PolicyConfig struct {
	Roles    []RoleDefinition   `json:"roles"`
	Policies []PolicyDefinition `json:"policies"`
}

// RoleDefinition defines a role with permissions
type RoleDefinition struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Permissions []string `json:"permissions"`
}

// PolicyDefinition defines method-level policies
type PolicyDefinition struct {
	Method      string   `json:"method"`
	Permissions []string `json:"permissions"`
	Description string   `json:"description"`
}

// PolicyManager manages RBAC policies from configuration
type PolicyManager struct {
	mu          sync.RWMutex
	config      *PolicyConfig
	logger      *zap.Logger
	auditLogger *zap.Logger
}

// NewPolicyManager creates a new policy manager
func NewPolicyManager(logger *zap.Logger) *PolicyManager {
	return &PolicyManager{
		config:      &PolicyConfig{},
		logger:      logger,
		auditLogger: logger.Named("policy"),
	}
}

// LoadFromFile loads policies from a JSON configuration file
func (pm *PolicyManager) LoadFromFile(filePath string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read policy file: %w", err)
	}

	var config PolicyConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("failed to parse policy file: %w", err)
	}

	pm.config = &config
	pm.logger.Info("Loaded RBAC policies from file",
		zap.String("file", filePath),
		zap.Int("roles", len(config.Roles)),
		zap.Int("policies", len(config.Policies)),
	)

	return nil
}

// LoadFromConfig loads policies from in-memory configuration
func (pm *PolicyManager) LoadFromConfig(config *PolicyConfig) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.config = config
	pm.logger.Info("Loaded RBAC policies from config",
		zap.Int("roles", len(config.Roles)),
		zap.Int("policies", len(config.Policies)),
	)
}

// GetRoles returns all defined roles
func (pm *PolicyManager) GetRoles() []RoleDefinition {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return pm.config.Roles
}

// GetPolicies returns all defined policies
func (pm *PolicyManager) GetPolicies() []PolicyDefinition {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return pm.config.Policies
}

// GetRolePermissions returns permissions for a specific role
func (pm *PolicyManager) GetRolePermissions(roleName string) ([]string, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	for _, role := range pm.config.Roles {
		if role.Name == roleName {
			return role.Permissions, nil
		}
	}

	return nil, fmt.Errorf("role not found: %s", roleName)
}

// GetMethodPermissions returns required permissions for a method
func (pm *PolicyManager) GetMethodPermissions(method string) ([]string, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	for _, policy := range pm.config.Policies {
		if policy.Method == method {
			return policy.Permissions, true
		}
	}

	return nil, false
}

// AddRole adds a new role definition (hot-reload support)
func (pm *PolicyManager) AddRole(role RoleDefinition) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Check if role exists, update if it does
	for i, r := range pm.config.Roles {
		if r.Name == role.Name {
			pm.config.Roles[i] = role
			pm.auditLogger.Info("Updated role",
				zap.String("role", role.Name),
				zap.Int("permissions", len(role.Permissions)),
			)
			return
		}
	}

	// Add new role
	pm.config.Roles = append(pm.config.Roles, role)
	pm.auditLogger.Info("Added new role",
		zap.String("role", role.Name),
		zap.Int("permissions", len(role.Permissions)),
	)
}

// AddPolicy adds a new method policy (hot-reload support)
func (pm *PolicyManager) AddPolicy(policy PolicyDefinition) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Check if policy exists, update if it does
	for i, p := range pm.config.Policies {
		if p.Method == policy.Method {
			pm.config.Policies[i] = policy
			pm.auditLogger.Info("Updated policy",
				zap.String("method", policy.Method),
				zap.Int("permissions", len(policy.Permissions)),
			)
			return
		}
	}

	// Add new policy
	pm.config.Policies = append(pm.config.Policies, policy)
	pm.auditLogger.Info("Added new policy",
		zap.String("method", policy.Method),
		zap.Int("permissions", len(policy.Permissions)),
	)
}

// DefaultPolicyConfig returns default RBAC policies
func DefaultPolicyConfig() *PolicyConfig {
	return &PolicyConfig{
		Roles: []RoleDefinition{
			{
				Name:        "admin",
				Description: "Full system access",
				Permissions: []string{"admin:*"},
			},
			{
				Name:        "operator",
				Description: "Job management and monitoring",
				Permissions: []string{
					"job:submit",
					"job:cancel",
					"job:view",
					"job:list",
					"metrics:view",
				},
			},
			{
				Name:        "viewer",
				Description: "Read-only access",
				Permissions: []string{
					"job:view",
					"job:list",
					"metrics:view",
				},
			},
			{
				Name:        "worker",
				Description: "Worker node access",
				Permissions: []string{
					"job:view",
					"job:list",
					"job:update",
				},
			},
		},
		Policies: []PolicyDefinition{
			{
				Method:      "/proto.MasterService/SubmitJob",
				Permissions: []string{"job:submit"},
				Description: "Submit new jobs",
			},
			{
				Method:      "/proto.MasterService/CancelJob",
				Permissions: []string{"job:cancel"},
				Description: "Cancel running jobs",
			},
			{
				Method:      "/proto.MasterService/GetJobStatus",
				Permissions: []string{"job:view"},
				Description: "View job status",
			},
			{
				Method:      "/proto.MasterService/ListJobs",
				Permissions: []string{"job:list"},
				Description: "List all jobs",
			},
			{
				Method:      "/proto.MasterService/JoinCluster",
				Permissions: []string{"cluster:manage"},
				Description: "Join cluster as node",
			},
			{
				Method:      "/proto.WorkerService/AssignJob",
				Permissions: []string{"job:update"},
				Description: "Assign job to worker",
			},
		},
	}
}
