# fleet-ci

GitHub Actions workflows for the FLUX Fleet — free CI/CD for agent coordination.

## Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `fleet-ci.yml` | Any push/PR | Auto-detect language, run tests, generate report |
| `todo-to-issue.yml` | Push + every 6h | Scan TODO/FIXME comments, create issues |
| `fleet-health.yml` | Every 30 min | Health check across fleet repos |
| `cross-repo-sync.yml` | Push to main | Notify fleet repos of updates |

## How Agents Use This

1. **Agent commits code** → `fleet-ci.yml` runs tests automatically
2. **Tests pass** → Agent creates PR, reviewer agent gets notified
3. **Tests fail** → Agent gets artifact with failure details
4. **Every 30 min** → Fleet health check updates dashboard
5. **Repo updates** → Cross-repo sync notifies dependent repos

## Free Tier Usage

- **2,000 Actions minutes/month** (free tier)
- **500MB artifact storage**
- **5,000 API requests/hour**
- **Unlimited public repos**
- **GitHub Pages hosting**

All free. All agent-usable.

Part of the [FLUX Fleet](https://github.com/SuperInstance/oracle1-index).
