package security

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestLoadFromFile(t *testing.T) {
	// Create temporary policy file
	tmpDir := t.TempDir()
	policyFile := filepath.Join(tmpDir, "policies.json")

	policyJSON := `{
		"roles": [
			{
				"name": "admin",
				"description": "Administrator",
				"permissions": ["job:submit", "job:view", "job:cancel"]
			},
			{
				"name": "viewer",
				"description": "Read-only",
				"permissions": ["job:view"]
			}
		],
		"policies": [
			{
				"method": "/scheduler.Scheduler/SubmitJob",
				"permissions": ["job:submit"],
				"description": "Submit job"
			},
			{
				"method": "/scheduler.Scheduler/GetJobStatus",
				"permissions": ["job:view"],
				"description": "Get job status"
			}
		]
	}`

	err := os.WriteFile(policyFile, []byte(policyJSON), 0644)
	require.NoError(t, err)

	// Load policies
	logger := zaptest.NewLogger(t)
	pm := NewPolicyManager(logger)
	err = pm.LoadFromFile(policyFile)
	require.NoError(t, err)

	// Verify roles
	roles := pm.GetRoles()
	assert.Len(t, roles, 2)

	roleNames := make([]string, len(roles))
	for i, role := range roles {
		roleNames[i] = role.Name
	}
	assert.Contains(t, roleNames, "admin")
	assert.Contains(t, roleNames, "viewer")

	// Verify role permissions
	adminPerms, err := pm.GetRolePermissions("admin")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"job:submit", "job:view", "job:cancel"}, adminPerms)

	viewerPerms, err := pm.GetRolePermissions("viewer")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"job:view"}, viewerPerms)

	// Verify policies
	policies := pm.GetPolicies()
	assert.Len(t, policies, 2)

	// Verify method permissions
	submitPerms, ok := pm.GetMethodPermissions("/scheduler.Scheduler/SubmitJob")
	require.True(t, ok)
	assert.ElementsMatch(t, []string{"job:submit"}, submitPerms)

	statusPerms, ok := pm.GetMethodPermissions("/scheduler.Scheduler/GetJobStatus")
	require.True(t, ok)
	assert.ElementsMatch(t, []string{"job:view"}, statusPerms)
}

func TestLoadFromFile_FileNotFound(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pm := NewPolicyManager(logger)
	err := pm.LoadFromFile("/nonexistent/path/policies.json")
	assert.Error(t, err)
}

func TestLoadFromFile_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	policyFile := filepath.Join(tmpDir, "invalid.json")

	err := os.WriteFile(policyFile, []byte("invalid json{"), 0644)
	require.NoError(t, err)

	logger := zaptest.NewLogger(t)
	pm := NewPolicyManager(logger)
	err = pm.LoadFromFile(policyFile)
	assert.Error(t, err)
}

func TestLoadFromConfig(t *testing.T) {
	config := PolicyConfig{
		Roles: []RoleDefinition{
			{
				Name:        "operator",
				Description: "Operator role",
				Permissions: []string{"job:submit", "job:view"},
			},
		},
		Policies: []PolicyDefinition{
			{
				Method:      "/scheduler.Scheduler/SubmitJob",
				Permissions: []string{"job:submit"},
				Description: "Submit job",
			},
		},
	}

	logger := zaptest.NewLogger(t)
	pm := NewPolicyManager(logger)
	pm.LoadFromConfig(&config)

	// Verify loaded
	roles := pm.GetRoles()
	assert.Len(t, roles, 1)

	roleNames := make([]string, len(roles))
	for i, role := range roles {
		roleNames[i] = role.Name
	}
	assert.Contains(t, roleNames, "operator")

	perms, err := pm.GetRolePermissions("operator")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"job:submit", "job:view"}, perms)
}

func TestDefaultPolicyConfig(t *testing.T) {
	config := DefaultPolicyConfig()

	// Verify default roles
	assert.Len(t, config.Roles, 4)

	roleNames := make([]string, len(config.Roles))
	for i, role := range config.Roles {
		roleNames[i] = role.Name
	}
	assert.ElementsMatch(t, []string{"admin", "operator", "viewer", "worker"}, roleNames)

	// Verify admin has all permissions (wildcard)
	var adminRole RoleDefinition
	for _, role := range config.Roles {
		if role.Name == "admin" {
			adminRole = role
			break
		}
	}
	assert.Contains(t, adminRole.Permissions, "admin:*")

	// Verify default policies
	assert.NotEmpty(t, config.Policies)
}

func TestAddRole(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pm := NewPolicyManager(logger)

	// Add a role
	role := RoleDefinition{
		Name:        "custom",
		Description: "Custom role",
		Permissions: []string{"custom:perm1", "custom:perm2"},
	}
	pm.AddRole(role)

	// Verify role added
	roles := pm.GetRoles()
	roleNames := make([]string, len(roles))
	for i, role := range roles {
		roleNames[i] = role.Name
	}
	assert.Contains(t, roleNames, "custom")

	perms, err := pm.GetRolePermissions("custom")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"custom:perm1", "custom:perm2"}, perms)
}

func TestAddRole_Update(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pm := NewPolicyManager(logger)

	// Add initial role
	role1 := RoleDefinition{
		Name:        "custom",
		Description: "Custom role",
		Permissions: []string{"perm1"},
	}
	pm.AddRole(role1)

	// Update role with new permissions
	role2 := RoleDefinition{
		Name:        "custom",
		Description: "Updated custom role",
		Permissions: []string{"perm1", "perm2"},
	}
	pm.AddRole(role2)

	// Verify updated
	perms, err := pm.GetRolePermissions("custom")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"perm1", "perm2"}, perms)
}

func TestAddPolicy(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pm := NewPolicyManager(logger)

	// Add a policy
	policy := PolicyDefinition{
		Method:      "/custom.Service/Method",
		Permissions: []string{"custom:perm"},
		Description: "Custom policy",
	}
	pm.AddPolicy(policy)

	// Verify policy added
	policies := pm.GetPolicies()
	found := false
	for _, p := range policies {
		if p.Method == "/custom.Service/Method" {
			found = true
			break
		}
	}
	assert.True(t, found)

	perms, ok := pm.GetMethodPermissions("/custom.Service/Method")
	require.True(t, ok)
	assert.ElementsMatch(t, []string{"custom:perm"}, perms)
}

func TestGetRolePermissions_NotFound(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pm := NewPolicyManager(logger)

	_, err := pm.GetRolePermissions("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "role not found")
}

func TestGetMethodPermissions_NotFound(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pm := NewPolicyManager(logger)

	_, ok := pm.GetMethodPermissions("/nonexistent.Service/Method")
	assert.False(t, ok)
}

func TestConcurrentAccess(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pm := NewPolicyManager(logger)
	pm.LoadFromConfig(DefaultPolicyConfig())

	// Simulate concurrent reads
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			_ = pm.GetRoles()
			_ = pm.GetPolicies()
			_, _ = pm.GetRolePermissions("admin")
			_, _ = pm.GetMethodPermissions("/scheduler.Scheduler/SubmitJob")
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}
