Sentinel timer responsibilities:

- send periodic PING commands to monitored instances
- send INFO commands to masters to discover replicas and sentinels
- evaluate SDOWN and ODOWN based on down-after and quorum settings
- drive failover progress from the event-loop cron path
