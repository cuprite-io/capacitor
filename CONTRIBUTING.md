# Contributing to Capacitor

First off, thank you for considering contributing to Capacitor! It's people like you that make it a great tool.

## Technical Philosophy

- **Local-First**: The network should never be on the hot path for reads or writes.
- **Efficiency**: Minimize allocations. Use pooled buffers and binary serialization (`msgp`).
- **Safety**: Distributed systems are hard. New features must include chaos and concurrency tests.

## Development Workflow

1. **Fork the Repo**: Create a feature branch.
2. **Local Development**:
   - Ensure your code follows `go fmt`.
   - Run existing tests: `go test -v ./...`
3. **Testing**: 
   - If you add a feature, add a unit test in `capacitor_test.go`.
   - If it's a distributed feature, add a test in `convergence_test.go` or `chaos_test.go`.
4. **Pull Request**:
   - Provide a clear description of the change.
   - Ensure the CI (GitHub Actions) passes.

## Code of Conduct

Be respectful and professional. We aim to build a welcoming community for everyone.

## Reporting Bugs

Use GitHub Issues to report bugs. Provide:
- A clear description of the issue.
- Steps to reproduce.
- Environment details (Go version, OS).
```
