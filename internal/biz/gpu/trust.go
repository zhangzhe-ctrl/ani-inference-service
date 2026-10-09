package gpu

import "context"

type governanceIdentityKey struct{}

// WithGovernanceIdentity is called by the transport only after exact trusted
// Governance certificate verification. Caller metadata does not grant identity.
func WithGovernanceIdentity(ctx context.Context) context.Context {
	return context.WithValue(ctx, governanceIdentityKey{}, true)
}

func GovernanceIdentity(ctx context.Context) bool {
	trusted, _ := ctx.Value(governanceIdentityKey{}).(bool)
	return trusted
}
