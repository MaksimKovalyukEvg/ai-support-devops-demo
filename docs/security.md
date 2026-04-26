# Security Notes

This repository is prepared as a sanitized demo.

## Excluded from Git

- Environment files: `.env`, `.env.*`
- API keys and service tokens
- Browser sessions and cookies
- Logs and runtime state
- Database files and exports
- Real customer dialogs and personal data
- Production domains, IP addresses and private URLs

## Sensitive data patterns

The production version should mask:

- email addresses
- phone numbers
- names
- Telegram usernames
- links
- IDs and transaction numbers
- internal URLs

## Recommended checks before push

Run this before publishing:

    grep -R "OPENAI_API_KEY\|TOKEN\|PASSWORD\|SECRET\|toycreative\|hmbserv\|getcourse" . --exclude-dir=.git

Recommended secret scanner:

    gitleaks detect --source .
