# Linkedin Marketing Plugin
This Dataiku DSS plugin retrieves information and metrics about LinkedIn's campaigns using [LinkedIn Marketing Developper platform](https://docs.microsoft.com/en-us/linkedin/marketing/).


## Output
Retrieve 5 datasets which are useful to analyse marketing campaigns 
- `campaign_groups` : categories of campaigns. In fact, campaigns are often sorted by country or by topics (France/ Partners..)
- `campaigns`  : returns the names of the campaigns (ex - AI in Banking WP - May 2020) , their targeting (useful to infer personas), budget ... 
- `creatives `: returns information about the existing creatives such as their status (active, paused..), their types..
- `campaign_analytics` : daily metrics (impressions, conversions, cost ) at a campaign level  
- `creative analytics` :  daily metrics (impressions, conversions, cost ) at a creative level  

## Authentification
To use this plugin, you need authentify first with [LinkedIn's 3-legged authentification](https://docs.microsoft.com/en-us/linkedin/shared/authentication/authorization-code-flow?context=linkedin/marketing/context). 
An access token is therefore retrieved. Please save it as a preset of the Plugin. 