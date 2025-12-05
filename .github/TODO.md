# GitHub Actions TODOs

## Improvements

- [ ] Replace manual Trivy installation with official Trivy GitHub Action
  - Current: Manual installation via apt in `promote-images.yml`
  - Better: Use `aquasecurity/trivy-action@master` 
  - Files to update: `.github/workflows/promote-images.yml` (2 locations)
  - Benefits: Faster, more reliable, better caching
