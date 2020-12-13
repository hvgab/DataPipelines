# DataPipelines

- Find the old MIE scripts and use the autoexport script.
- Build up the models for "logging"/history
  - sftp-servers
  - notifications to-emails/teams-channels
  - one export is
    - extract: default sql / custom sql
    - optional custom transforms (pre transform)
    - default transforms (the pandas join from autoexport script)
    - optional custom transforms (post transform)
    - graph/pipe needs to split, send file to multiple entities? internal notification / saving, and external notification / saving
- Consider setting up some etl to consolidate the different servers.
- Config can stay in the django admin pages for elevated users.
- Custom overview and setup for simple exports for alle team-leads.
