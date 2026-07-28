## Summary

<!-- What changed and why? -->

## Verification

- [ ] `make check`
- [ ] `make docker-test`
- [ ] Terraform plan reviewed when infrastructure changed

## Safety

- [ ] No health values, OAuth material, webhook payloads, or Notion content appear in logs or fixtures
- [ ] Data-format changes include schema and backward-compatibility review
- [ ] HEAD CAS remains the only commit point
