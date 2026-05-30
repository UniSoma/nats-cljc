// Karma harness for the shadow-cljs :karma target (browser-headless CI job).
// Consumes target/karma-test.js, runs cljs.test in headless Chrome.
module.exports = function (config) {
  config.set({
    browsers: ['ChromeHeadlessCI'],
    // GitHub Actions runs Chrome as root, which requires --no-sandbox.
    customLaunchers: {
      ChromeHeadlessCI: {
        base: 'ChromeHeadless',
        flags: ['--no-sandbox', '--disable-gpu']
      }
    },
    basePath: 'target',
    files: ['karma-test.js'],
    frameworks: ['cljs-test'],
    plugins: ['karma-cljs-test', 'karma-chrome-launcher'],
    colors: true,
    logLevel: config.LOG_INFO,
    client: {
      args: ['shadow.test.karma.init']
    },
    singleRun: true
  });
};
