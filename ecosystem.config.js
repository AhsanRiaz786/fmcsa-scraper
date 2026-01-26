module.exports = {
  apps: [{
    name: 'fmcsa-scraper',
    script: 'dist/orchestrator.js',
    instances: 1,  // Single instance (concurrency handled internally)
    exec_mode: 'fork',
    max_memory_restart: '6G',
    env: {
      NODE_ENV: 'production',
      NODE_OPTIONS: '--max-old-space-size=6144'
    },
    error_file: './logs/pm2-error.log',
    out_file: './logs/pm2-out.log',
    log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    merge_logs: true,
    autorestart: true,
    watch: false
  }]
};
