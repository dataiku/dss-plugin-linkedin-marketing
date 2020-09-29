# Linkedin Marketing Plugin
This Dataiku DSS plugin retrieves information and metrics about LinkedIn's campaigns using [LinkedIn Marketing Developper platform](https://docs.microsoft.com/en-us/linkedin/marketing/).

## Authentification
To use this plugin, you need authentify first with [LinkedIn's 3-legged authentification](https://docs.microsoft.com/en-us/linkedin/shared/authentication/authorization-code-flow?context=linkedin/marketing/context). 
An access token is therefore retrieved. Please save it as a preset of the Plugin. 

## Input
No datasets but parameters : 
- **account id** : Ids of the sponsored accounts that you want to retrieve. For multiple accounts, separate the ids with a comma using the following format : id1,id2,id3. You may find the list of accounts in the [LinkedIn Campaign Manager tool](https://www.linkedin.com/campaignmanager/accounts). To retrieve all the tables, the account should contain at least one campaign and one creative
- A **timeline** : date range for the Analytics data that you want to retrieve.Format dd/MM/YYYY
- **Batch size** : to query the ad analytics API, pagination is not handled. That is why, batch queries are performed. Hence, the recipe filters each query using a given number of campaign or creative ids. These ids are retrieved from the `campaigns` dataset for `campaign_analytics`and `creatives`for `creative_analytics`. If the API returns an error saying that you requested too much data, decrease your batchsize. If you think that your recipe is too slow, increase the batchsize. Normally, if the timeline is short, (1 month) you may use a bigger batchsize. There is no default value for it depends on the activities of your different ad accounts. 
- **retrieve raw response**: if ticked, add a `raw_response` column with the raw jsons retrieved from the API, without any formatting. 
- **access token** : preset, that you need to create from the settings of the plugin. 


## Output
Retrieve 5 datasets which are useful to analyse marketing campaigns 
- `campaign_groups` : categories of campaigns. In fact, campaigns are often sorted by country or by topics (France/ Partners..)
- `campaigns`  : returns the names of the campaigns, their targeting, budget ... 
- `creatives `: returns information about the existing creatives such as their status (active, paused..), their types..
- `campaign_analytics` : daily metrics (impressions, conversions, cost ) at a campaign level  
- `creative analytics` :  daily metrics (impressions, conversions, cost ) at a creative level  
