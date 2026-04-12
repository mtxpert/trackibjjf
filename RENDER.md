# Render Deployment

## API

- **API Key**: `rnd_oZSi3uNncCYYVX6XUBvIsHKT7hMR`
- **Owner ID**: `tea-d6q2di4r85hc73c1sue0` (My Workspace — mbambic@gmail.com)
- **Rotate key**: dashboard.render.com → Account Settings → API Keys

## Services

| Name | Service ID | URL |
|------|------------|-----|
| trackibjjf | `srv-d7dq9k5ckfvc73f0natg` | https://trackibjjf.onrender.com |
| gb-ads-platform | `srv-d6s7des50q8c73feo76g` | — |
| epp-inventory | `srv-d6q2fmh4tr6s73ae3nc0` | https://epp-inventory.onrender.com |
| epp-chatbot | `srv-d6teljkhg0os73fp5vvg` | https://epp-chatbot.onrender.com |
| epp-retail | `srv-d71enr4r85hc73a0aub0` | https://epp-retail.onrender.com |

## Common Commands

```bash
# Trigger a deploy
curl -X POST \
  -H "Authorization: Bearer rnd_oZSi3uNncCYYVX6XUBvIsHKT7hMR" \
  https://api.render.com/v1/services/srv-d7dq9k5ckfvc73f0natg/deploys

# Check deploy status
curl -H "Authorization: Bearer rnd_oZSi3uNncCYYVX6XUBvIsHKT7hMR" \
  https://api.render.com/v1/services/srv-d7dq9k5ckfvc73f0natg/deploys?limit=1

# List all services
curl -H "Authorization: Bearer rnd_oZSi3uNncCYYVX6XUBvIsHKT7hMR" \
  https://api.render.com/v1/services
```
