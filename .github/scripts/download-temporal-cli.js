/**
 * Download and verify Temporal CLI binaries for multiple architectures
 * @param {object} require - Node.js require function
 */
module.exports = async ({ require }) => {
  const fs = require('fs');
  const path = require('path');
  const { execFileSync } = require('child_process');
  const crypto = require('crypto');

  const CLI_VERSION = '1.5.0';

  // Validate CLI_VERSION format to prevent injection
  if (!/^[0-9]+\.[0-9]+\.[0-9]+$/.test(CLI_VERSION)) {
    throw new Error(`Invalid CLI_VERSION format: ${CLI_VERSION}`);
  }

  const archs = [
    { name: 'amd64', release: 'x86_64' },
    { name: 'arm64', release: 'arm64' }
  ];

  // Download checksums file first
  const checksumUrl = `https://github.com/temporalio/cli/releases/download/v${CLI_VERSION}/checksums.txt`;
  console.log(`Downloading checksums from ${checksumUrl}`);

  try {
    execFileSync('curl', ['-fsSL', '-o', '/tmp/cli-checksums.txt', checksumUrl]);
  } catch (error) {
    throw new Error(`Failed to download checksums: ${error.message}`);
  }

  // Read checksums
  const checksumsContent = fs.readFileSync('/tmp/cli-checksums.txt', 'utf8');
  const checksums = new Map();
  checksumsContent.split('\n').forEach(line => {
    const match = line.match(/^([a-f0-9]{64})\s+(.+)$/);
    if (match) {
      checksums.set(match[2], match[1]);
    }
  });

  for (const arch of archs) {
    const tarballName = `temporal_cli_${CLI_VERSION}_linux_${arch.release}.tar.gz`;
    const downloadUrl = `https://github.com/temporalio/cli/releases/download/v${CLI_VERSION}/${tarballName}`;
    const tempPath = `/tmp/${tarballName}`;

    console.log(`Downloading Temporal CLI for ${arch.name} from ${downloadUrl}`);

    // Download tarball using execFileSync to prevent command injection
    try {
      execFileSync('curl', ['-fsSL', '-o', tempPath, downloadUrl]);
    } catch (error) {
      throw new Error(`Failed to download ${tarballName}: ${error.message}`);
    }

    // Verify checksum
    const expectedChecksum = checksums.get(tarballName);
    if (!expectedChecksum) {
      throw new Error(`No checksum found for ${tarballName}`);
    }

    const fileBuffer = fs.readFileSync(tempPath);
    const actualChecksum = crypto.createHash('sha256').update(fileBuffer).digest('hex');

    if (actualChecksum !== expectedChecksum) {
      throw new Error(`Checksum mismatch for ${tarballName}. Expected: ${expectedChecksum}, Got: ${actualChecksum}`);
    }

    console.log(`Checksum verified for ${tarballName}`);

    // Extract binary using execFileSync
    try {
      execFileSync('tar', ['-xzf', tempPath, '-C', '/tmp', 'temporal']);
    } catch (error) {
      throw new Error(`Failed to extract ${tarballName}: ${error.message}`);
    }

    // Move to build directory
    const destDir = `build/${arch.name}`;
    if (!fs.existsSync(destDir)) {
      fs.mkdirSync(destDir, { recursive: true });
    }

    fs.renameSync('/tmp/temporal', `${destDir}/temporal`);
    fs.chmodSync(`${destDir}/temporal`, 0o755);

    console.log(`Installed Temporal CLI to ${destDir}/temporal`);

    // Clean up
    fs.unlinkSync(tempPath);
  }

  // Clean up checksums file
  fs.unlinkSync('/tmp/cli-checksums.txt');
};
