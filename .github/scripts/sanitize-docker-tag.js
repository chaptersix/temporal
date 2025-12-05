/**
 * Sanitize branch name to create a safe Docker tag
 * - Replace non-alphanumeric (except .-_) with dash
 * - Convert to lowercase
 * - Start with alphanumeric
 * - Truncate to 128 characters
 *
 * @param {object} core - GitHub Actions core utilities
 * @param {string} branchName - The branch name to sanitize
 */
module.exports = async ({ core, branchName }) => {
  // Replace any non-alphanumeric (except .-_) with dash
  let safeTag = branchName.replace(/[^a-zA-Z0-9._-]/g, '-');

  // Docker tags must be lowercase
  safeTag = safeTag.toLowerCase();

  // Docker tags must start with alphanumeric
  safeTag = safeTag.replace(/^[^a-z0-9]+/, '');

  // Truncate to 128 characters (Docker tag limit)
  safeTag = safeTag.substring(0, 128);

  console.log(`Original: ${branchName}`);
  console.log(`Sanitized: ${safeTag}`);

  core.setOutput('tag', safeTag);
};
