/**
 * Organize downloaded binaries for Docker build
 * Copies binaries from artifact directories to build directory
 * @param {object} require - Node.js require function
 */
module.exports = async ({ require }) => {
  const fs = require('fs');
  const path = require('path');

  // Create build directory structure
  ['amd64', 'arm64'].forEach(arch => {
    const srcDir = `build-artifacts/${arch}`;
    const destDir = `build/${arch}`;

    if (!fs.existsSync(destDir)) {
      fs.mkdirSync(destDir, { recursive: true });
    }

    // Copy binaries
    if (fs.existsSync(srcDir)) {
      fs.readdirSync(srcDir).forEach(file => {
        const srcFile = path.join(srcDir, file);
        const destFile = path.join(destDir, file);

        if (fs.statSync(srcFile).isFile()) {
          fs.copyFileSync(srcFile, destFile);
          // Make executable
          fs.chmodSync(destFile, 0o755);
          console.log(`Copied ${srcFile} -> ${destFile}`);
        }
      });
    }
  });

  // Copy schema directory (should be in amd64 artifact)
  const schemaSource = 'build-artifacts/amd64/temporal/schema';
  const schemaDest = 'build/temporal/schema';

  if (fs.existsSync(schemaSource)) {
    const copyRecursive = (src, dest) => {
      if (!fs.existsSync(dest)) {
        fs.mkdirSync(dest, { recursive: true });
      }
      fs.readdirSync(src).forEach(item => {
        const srcPath = path.join(src, item);
        const destPath = path.join(dest, item);
        if (fs.statSync(srcPath).isDirectory()) {
          copyRecursive(srcPath, destPath);
        } else {
          fs.copyFileSync(srcPath, destPath);
        }
      });
    };
    copyRecursive(schemaSource, schemaDest);
    console.log('Copied schema directory');
  }
};
