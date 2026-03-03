# Permissions

Use this checklist for a minimal read-only install.

## 1) Required Bot Permissions

- `View Channels`
- `Read Message History`

These correspond to invite bitmask `permissions=66560`.

## 2) Optional Permissions

- `View Audit Log`
  - Needed only for reliable kick/moderator attribution in lifecycle events.
- `Manage Threads`
  - Needed only for private archived-thread backfill coverage.

## 3) Required Privileged Gateway Intents (Developer Portal -> Bot)

- `Server Members Intent`
- `Message Content Intent`

## 4) OAuth2 URL Generator Steps

In Discord Developer Portal for your application:

1. Open `OAuth2 -> URL Generator`.
2. Under **Scopes**, tick `bot`.
3. Under **Bot Permissions**, tick:
   - `View Channels`
   - `Read Message History`
4. Under **Integration Type**, choose `Guild Install`.
5. Open the generated URL and invite the bot to your server.

If you need a direct URL, use this template:

```text
https://discord.com/oauth2/authorize?client_id=<APPLICATION_ID>&permissions=66560&integration_type=0&scope=bot
```

Replace `<APPLICATION_ID>` with your app's Application ID from `General Information`.

After joining the server, if your server uses restrictive role/channel overwrites, create a read-only role with:

- `View Channels`
- `Read Message History`

And assign that role to the bot.
