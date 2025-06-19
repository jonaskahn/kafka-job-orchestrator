module.exports = {
  env: {
    node: true,
    es2021: true,
  },
  extends: [
    "eslint:recommended",
    "plugin:node/recommended",
    "prettier", // Must be last to override other configs
  ],
  plugins: ["node"],
  parserOptions: {
    ecmaVersion: 2021,
    sourceType: "module",
  },
  rules: {
    // Error prevention
    "no-console": "off", // Allow console.log in Node.js
    "no-unused-vars": ["error", { argsIgnorePattern: "^_" }],
    "no-var": "error",
    "prefer-const": "error",

    // Code style (handled by Prettier, but some logical rules)
    "no-multiple-empty-lines": ["error", { max: 2, maxEOF: 1 }],
    "no-trailing-spaces": "error",
    "eol-last": "error",

    // Node.js specific
    "node/no-unsupported-features/es-syntax": "off", // Allow modern ES features
    "node/no-missing-require": "error",
    "node/no-unpublished-require": "off", // Allow devDependencies in scripts
    "node/prefer-global/process": "error",
    "node/prefer-global/buffer": "error",

    // Best practices
    "no-throw-literal": "error",
    "prefer-template": "error",
    "object-shorthand": "error",
    "no-param-reassign": "error",

    // Async/await - relaxed for abstract methods
    "no-async-promise-executor": "error",
    "require-await": "off", // Turned off as abstract methods may not need await
  },
  overrides: [
    {
      files: ["*.test.js", "tests/**/*.js"],
      env: {
        jest: true,
      },
      rules: {
        "node/no-unpublished-require": "off",
      },
    },
    {
      // Allow process.exit in run scripts and example files
      files: ["examples/**/*.js", "**/run-*.js", "scripts/**/*.js"],
      rules: {
        "no-process-exit": "off",
        "node/shebang": "off",
      },
    },
    {
      // Abstract classes can have methods with unused parameters
      files: ["src/abstracts/**/*.js"],
      rules: {
        "no-unused-vars": [
          "error",
          {
            args: "none", // Don't check arguments in abstract classes
            argsIgnorePattern: "^_",
          },
        ],
      },
    },
  ],
};
