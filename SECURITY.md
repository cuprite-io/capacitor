# Security Policy

## Supported Versions

We currently support and provide security updates for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| v0.1.x  | :white_check_mark: |

## Reporting a Vulnerability

We take the security of Capacitor seriously. If you believe you have found a security vulnerability, please do not open a public issue. Instead, please report it via the following process:

1. **Email us**: Send a detailed report to [security@cuprite.io](mailto:security@cuprite.io).
2. **Details**: Include a description of the vulnerability, a proof of concept, and the potential impact.
3. **Response**: You will receive an acknowledgment within 48 hours.
4. **Disclosure**: We follow coordinated disclosure. We will work with you to fix the issue before making it public.

## Security Features

Capacitor provides several built-in security features:
- **mTLS**: Encrypted and authenticated replication streams.
- **Gossip Security**: Shared-secret authentication for node discovery.
- **Clock Smash Protection**: Prevents malicious or broken clocks from disrupting cluster causality.
```
