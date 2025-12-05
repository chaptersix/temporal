/**
 * Organize GoReleaser binaries for Docker build
 * Copies binaries from dist/ to build/ directory with proper architecture structure
 * @param {object} require - Node.js require function
 */
module.exports = async ({ require }) => {
  const fs = require('fs');
  const path = require('path');

  // Helper function to validate paths and prevent traversal
  const validatePath = (filePath, allowedPrefix) => {
    const normalized = path.normalize(filePath);
    const resolved = path.resolve(normalized);
    const allowedResolved = path.resolve(allowedPrefix);

    // Check for path traversal
    if (normalized.includes('..')) {
      throw new Error(`Path traversal detected in: ${filePath}`);
    }

    // Ensure path is within allowed directory
    if (!resolved.startsWith(allowedResolved)) {
      throw new Error(`Path outside allowed directory: ${filePath}`);
    }

    return normalized;
  };

  // Create build directories
  const archs = ['amd64', 'arm64'];
  const binaries = [
    'temporal-server',
    'temporal-cassandra-tool',
    'temporal-sql-tool',
    'temporal-elasticsearch-tool',
    'tdbg'
  ];

  // Validate architecture and binary names
  archs.forEach(arch => {
    if (!/^[a-z0-9]+$/.test(arch)) {
      throw new Error(`Invalid architecture name: ${arch}`);
    }
  });

  binaries.forEach(binary => {
    if (!/^[a-z0-9-]+$/.test(binary)) {
      throw new Error(`Invalid binary name: ${binary}`);
    }
  });

  archs.forEach(arch => {
    const dir = `build/${arch}`;
    validatePath(dir, 'build');
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
  });

  // Map GoReleaser dist structure to build structure
  const archMap = {
    'amd64': 'amd64_v1',
    'arm64': 'arm64'
  };

  binaries.forEach(binary => {
    archs.forEach(arch => {
      const distArch = archMap[arch] || arch;
      const distPath = `dist/${binary}_linux_${distArch}/${binary}`;
      const buildPath = `build/${arch}/${binary}`;

      // Validate paths before file operations
      try {
        validatePath(distPath, 'dist');
        validatePath(buildPath, 'build');
      } catch (error) {
        console.error(`Path validation failed: ${error.message}`);
        throw error;
      }

      if (fs.existsSync(distPath)) {
        fs.copyFileSync(distPath, buildPath);
        console.log(`Copied ${distPath} -> ${buildPath}`);
      } else {
        console.warn(`Binary not found: ${binary} for ${arch}`);
      }
    });
  });

  // Copy schema directory for admin-tools
  const schemaDir = 'build/temporal/schema';
  validatePath(schemaDir, 'build');
  if (!fs.existsSync(schemaDir)) {
    fs.mkdirSync(schemaDir, { recursive: true });
  }

  // Copy all schema files recursively with path validation
  const copyRecursive = (src, dest) => {
    // Validate both source and destination
    validatePath(src, 'schema');
    validatePath(dest, 'build');

    if (fs.statSync(src).isDirectory()) {
      if (!fs.existsSync(dest)) {
        fs.mkdirSync(dest, { recursive: true });
      }
      fs.readdirSync(src).forEach(item => {
        // Validate item name to prevent directory traversal
        if (item.includes('..') || item.includes('/') || item.includes('\\')) {
          throw new Error(`Invalid file name: ${item}`);
        }
        copyRecursive(path.join(src, item), path.join(dest, item));
      });
    } else {
      fs.copyFileSync(src, dest);
    }
  };

  if (fs.existsSync('schema')) {
    copyRecursive('schema', schemaDir);
    console.log('Copied schema directory');
  }
};
