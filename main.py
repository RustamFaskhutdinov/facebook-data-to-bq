from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adaccountuser import AdAccountUser
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adset import AdSet
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import date, timedelta, datetime
import requests
import logging
import json
import base64
import time
import os
import pandas as pd
import pytz

logger = logging.getLogger()

ny_timezone = pytz.timezone('America/New_York')
current_date = datetime.now(ny_timezone)
current_date_string = current_date.strftime("%Y-%m-%d")
yesterday_date = current_date - timedelta(days=1)
yesterday_string = yesterday_date.strftime("%Y-%m-%d")


facebook_credentials = json.loads(os.environ['facebook_credentials'])
APP_ID = facebook_credentials['APP_ID']
APP_SECRET = facebook_credentials['APP_SECRET']
ACCESS_TOKEN = facebook_credentials['ACCESS_TOKEN']

FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)
client = bigquery.Client()

FB_ACCOUNT_ID = os.environ['fb_account_id']
PROJECT_ID = os.environ['project_id']
DATASET_ID = 'facebook_data'
AUDIENCE_TABLE_ID = 'facebook_audience'
REGION_TABLE_ID = 'facebook_region'
AD_TABLE_ID = 'facebook_ad_info'
ADSET_TABLE_ID = 'facebook_adset_info'
LOCATION='US'


fields =[
     AdsInsights.Field.account_currency,
    AdsInsights.Field.account_name,        
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.adset_id,
    AdsInsights.Field.adset_name,
    AdsInsights.Field.ad_id,
    AdsInsights.Field.ad_name,
     
    AdsInsights.Field.optimization_goal,
    AdsInsights.Field.objective,
    
    AdsInsights.Field.video_play_retention_0_to_15s_actions,
    AdsInsights.Field.video_play_retention_20_to_60s_actions,
    AdsInsights.Field.video_time_watched_actions,
    AdsInsights.Field.video_continuous_2_sec_watched_actions,
    AdsInsights.Field.video_30_sec_watched_actions,
    AdsInsights.Field.video_avg_time_watched_actions, 
    AdsInsights.Field.video_p25_watched_actions,
    AdsInsights.Field.video_p50_watched_actions,
    AdsInsights.Field.video_p75_watched_actions,
    AdsInsights.Field.video_p95_watched_actions,
    AdsInsights.Field.video_play_actions,
    
    AdsInsights.Field.clicks,
    AdsInsights.Field.unique_clicks,
    AdsInsights.Field.inline_link_clicks,
    AdsInsights.Field.unique_inline_link_clicks,
    AdsInsights.Field.outbound_clicks,
    AdsInsights.Field.unique_outbound_clicks,
     AdsInsights.Field.impressions,
     AdsInsights.Field.full_view_impressions,
     # AdsInsights.Field.reach,
     # AdsInsights.Field.frequency,
     AdsInsights.Field.spend,
     
     AdsInsights.Field.action_values,
     AdsInsights.Field.actions,
     AdsInsights.Field.conversion_values,
     AdsInsights.Field.conversions,
]

breakdowns_audience = ['age', 'gender',]
breakdowns_region = ['country', 'region',]

schema_facebook_audience = [
    bigquery.SchemaField("date_start", "DATE", mode="REQUIRED"),

    bigquery.SchemaField("account_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("account_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("account_currency", "STRING", mode="NULLABLE"),
    #ad
    bigquery.SchemaField("ad_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ad_name", "STRING", mode="NULLABLE"),
    #adset
    bigquery.SchemaField("adset_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("adset_name", "STRING", mode="NULLABLE"),

    #campaign
    bigquery.SchemaField("campaign_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("campaign_name", "STRING", mode="NULLABLE"),

    bigquery.SchemaField("optimization_goal", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("objective", "STRING", mode="NULLABLE"),

    bigquery.SchemaField("age", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),

    bigquery.SchemaField("clicks", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("unique_clicks", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("inline_link_clicks", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("unique_inline_link_clicks", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("outbound_clicks", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("unique_outbound_clicks", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("impressions", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("full_view_impressions", "INTEGER", mode="NULLABLE"),
    # bigquery.SchemaField("frequency", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("spend", "FLOAT", mode="NULLABLE"),
	
	
    bigquery.SchemaField("video_play_retention_0_to_15s_actions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_play_retention_20_to_60s_actions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_time_watched_actions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_continuous_2_sec_watched_actions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_30_sec_watched_actions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_avg_time_watched_actions", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("video_p25_watched_actions", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("video_p50_watched_actions", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("video_p75_watched_actions", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("video_p95_watched_actions", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("video_play_actions", "INTEGER", mode="NULLABLE"),
	# bigquery.SchemaField("video_play_curve_actions", "INTEGER", mode="NULLABLE"),

    bigquery.SchemaField('conversions', 'RECORD', mode='REPEATED',
        fields=(bigquery.SchemaField('action_type', 'STRING'),
                bigquery.SchemaField('value', 'FLOAT'))),
    bigquery.SchemaField('conversion_values', 'RECORD', mode='REPEATED',
        fields=(bigquery.SchemaField('action_type', 'STRING'),
                bigquery.SchemaField('value', 'FLOAT'))),

    bigquery.SchemaField('actions', 'RECORD', mode='REPEATED',
        fields=(bigquery.SchemaField('action_type', 'STRING'),
                bigquery.SchemaField('value', 'FLOAT'))),
    bigquery.SchemaField('action_values', 'RECORD', mode='REPEATED',
        fields=(bigquery.SchemaField('action_type', 'STRING'),
                bigquery.SchemaField('value', 'FLOAT')))
]


clustering_fields_audience = ['campaign_id', 'campaign_name', 'age', 'gender']

schema_facebook_region = [
    bigquery.SchemaField("date_start", "DATE", mode="NULLABLE"),

    bigquery.SchemaField("account_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("account_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("account_currency", "STRING", mode="NULLABLE"),
    #ad
    bigquery.SchemaField("ad_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ad_name", "STRING", mode="NULLABLE"),
    #adset
    bigquery.SchemaField("adset_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("adset_name", "STRING", mode="NULLABLE"),

    #campaign
    bigquery.SchemaField("campaign_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("campaign_name", "STRING", mode="NULLABLE"),

    bigquery.SchemaField("optimization_goal", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("objective", "STRING", mode="NULLABLE"),
    
    bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("region", "STRING", mode="NULLABLE"),

    bigquery.SchemaField("clicks", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("unique_clicks", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("inline_link_clicks", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("unique_inline_link_clicks", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("outbound_clicks", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("unique_outbound_clicks", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("impressions", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("full_view_impressions", "INTEGER", mode="NULLABLE"),
    # bigquery.SchemaField("frequency", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("spend", "FLOAT", mode="NULLABLE"),
		
    bigquery.SchemaField("video_play_retention_0_to_15s_actions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_play_retention_20_to_60s_actions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_time_watched_actions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_continuous_2_sec_watched_actions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_30_sec_watched_actions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_avg_time_watched_actions", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("video_p25_watched_actions", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("video_p50_watched_actions", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("video_p75_watched_actions", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("video_p95_watched_actions", "INTEGER", mode="NULLABLE"),
	bigquery.SchemaField("video_play_actions", "INTEGER", mode="NULLABLE"),
	# bigquery.SchemaField("video_play_curve_actions", "INTEGER", mode="NULLABLE"),

    bigquery.SchemaField('conversions', 'RECORD', mode='REPEATED',
        fields=(bigquery.SchemaField('action_type', 'STRING'),
                bigquery.SchemaField('value', 'FLOAT'))),
    bigquery.SchemaField('conversion_values', 'RECORD', mode='REPEATED',
        fields=(bigquery.SchemaField('action_type', 'STRING'),
                bigquery.SchemaField('value', 'FLOAT'))),

    bigquery.SchemaField('actions', 'RECORD', mode='REPEATED',
        fields=(bigquery.SchemaField('action_type', 'STRING'),
                bigquery.SchemaField('value', 'FLOAT'))),
    bigquery.SchemaField('action_values', 'RECORD', mode='REPEATED',
        fields=(bigquery.SchemaField('action_type', 'STRING'),
                bigquery.SchemaField('value', 'FLOAT')))
]

clustering_fields_region = ['campaign_id', 'campaign_name', 'country', 'region']

schema_ad_info = [
    bigquery.SchemaField("date_upload", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("ad_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ad_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("preview_shareable_link", "STRING", mode="NULLABLE"),
]

schema_adset_info = [
    bigquery.SchemaField("date_upload", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("adset_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pixel_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("custom_event_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("custom_event_str", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pixel_rule", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("custom_conversion_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("page_id", "STRING", mode="NULLABLE"),
]





def fb_create_insights_request(account_id, app_id, app_secret, access_token, date_range, fields, breakdowns):
    """
    All variableas are string.
    data_range format
    dict('start_date':'2023-01-01', 'end_date': '2023-01-31')
    
    """
    FacebookAdsApi.init(app_id, app_secret, access_token)
    account = AdAccount('act_'+str(account_id))

    params={
        'level': 'ad',
        'time_range': {
            'since':  date_range.get('start_date'),
            'until': date_range.get('end_date')
        },
        'time_increment': 1,
        'breakdowns': breakdowns,
    }
    # Both Insights and Reportstats
    insights_async = account.get_insights(fields=fields, params=params, is_async=True)
    return insights_async

def get_async_report(insights_async, return_as_list=True):
    start_time = time.time()
    while True:
        job = insights_async.api_get()
        time.sleep(2)
        if job[AdReportRun.Field.async_percent_completion] == 100 and job[AdReportRun.Field.async_status] == 'Job Completed':
            time.sleep(2)
            break
        elif job[AdReportRun.Field.async_status] == "Job Failed":
            print(job)
            return 'Fail'
    
    
    if return_as_list:
        insights = list(insights_async.get_result())
        end_time = time.time()
        print(end_time - start_time)
        return insights
    else:
        insights = insights_async.get_result()
        end_time = time.time()
        print(end_time - start_time)
        return insights

def report_to_df(report_data):
    data = []
    for element in report_data:
        row = element.export_all_data()
        # row = element
        # action_values
        if row.get('action_values', False):
            for action_value in row['action_values']:
                action_value['value'] = float(action_value["value"])
        else:
            row['action_values'] = []
        ##########################################
        # actions
        if row.get('actions', False):
            for action in row['actions']:
                action['value'] = int(action["value"])
        else:
            row['conversions'] = []
        ###########################################
        # conversion values
        if row.get('conversion_values', False):
            for conversion_value in row['conversion_values']:
                conversion_value['value'] = float(conversion_value["value"])
        else:
            row['conversion_values'] = []
        ########################################
        # conversions
        if row.get('conversions', False):
            for conversion in row['conversions']:
                conversion['value'] = int(conversion["value"])
        else:
            row['conversions'] = []
        
        for v in ['clicks', 'impressions', 'unique_clicks', 'inline_link_clicks', 'unique_inline_link_clicks']:
            if row.get(v, False): row[v] = int(row[v]) 
            else: row[v] = 0

        if row.get('spend', False): row['spend'] = float(row['spend'])
        else: row['spend'] = 0

        for v in ["video_play_retention_0_to_15s_actions", "video_play_retention_20_to_60s_actions", "video_time_watched_actions",
                  "video_continuous_2_sec_watched_actions", "video_30_sec_watched_actions", "video_avg_time_watched_actions", 
                  "video_p25_watched_actions", "video_p50_watched_actions", "video_p75_watched_actions", "video_p95_watched_actions",
                  "video_play_actions", "unique_outbound_clicks", "outbound_clicks"]:
            if row.get(v, False): row[v] =  int(row[v][0]['value'])
        
        try:
            del row['date_stop']
        except:
            pass
        
        data.append(row)

    return pd.DataFrame(data)

def exist_dataset_table(client, project_id, dataset_id, table_id, schema, clustering_fields=None, location='EU', partition_field_name='date'):

    try:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        client.get_dataset(dataset_ref)  # Make an API request.

    except NotFound:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset = client.create_dataset(dataset)  # Make an API request.
        logger.info("Created dataset {}.{}".format(client.project, dataset.dataset_id))

    try:
        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
        client.get_table(table_ref)  # Make an API request.

    except NotFound:

        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)

        table = bigquery.Table(table_ref, schema=schema)

        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field_name
        )

        if clustering_fields is not None:
            table.clustering_fields = clustering_fields

        table = client.create_table(table)  # Make an API request.
        logger.info("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    return 'ok'


def main(event=None, context=None):
    exist_dataset_table(client, PROJECT_ID, DATASET_ID, AD_TABLE_ID,
        schema_ad_info,
        location=LOCATION,
        partition_field_name='date_upload')

    exist_dataset_table(client, PROJECT_ID, DATASET_ID, ADSET_TABLE_ID,
        schema_adset_info,
        location=LOCATION,
        partition_field_name='date_upload')

    

    report_audience = fb_create_insights_request(
        account_id=FB_ACCOUNT_ID,
        app_id=APP_ID,
        app_secret=APP_SECRET,
        access_token= ACCESS_TOKEN,
        date_range={'start_date': yesterday_string, 'end_date': yesterday_string},
        fields=fields,
        breakdowns=breakdowns_audience)

    report_region = fb_create_insights_request(
        account_id=FB_ACCOUNT_ID,
        app_id=APP_ID,
        app_secret=APP_SECRET,
        access_token= ACCESS_TOKEN,
        date_range={'start_date': yesterday_string, 'end_date': yesterday_string},
        fields=fields,
        breakdowns=breakdowns_region)

    ads = AdAccount(f'act_{FB_ACCOUNT_ID}').get_ads(
        [Ad.Field.name, Ad.Field.preview_shareable_link], {'date_preset': Ad.DatePreset.yesterday})
    df_ad_preview = pd.DataFrame(ads).rename(columns={'id': 'ad_id', 'name': 'ad_name'})
    df_ad_preview['date_upload'] = current_date_string

    ad_set_conv_data = []
    ad_sets = AdAccount(f'act_{FB_ACCOUNT_ID}').get_ad_sets(['promoted_object'])
    for ad_set_ in ad_sets:
        nested_dict = ad_set_.export_all_data()
        ad_set_conv_data.append(
            {'id': nested_dict['id'],
            **nested_dict.get('promoted_object', {}),
            })
    
    df_ad_set_conv = pd.DataFrame(ad_set_conv_data).rename(columns={'id': 'adset_id'})
    df_ad_set_conv['date_upload'] = current_date_string

    df_ad_set_conv.to_gbq(
        project_id=PROJECT_ID,
        destination_table=f"{DATASET_ID}.{ADSET_TABLE_ID}",
        if_exists='append',
        location=LOCATION)

    df_ad_preview.to_gbq(
        project_id=PROJECT_ID,
        destination_table=f"{DATASET_ID}.{AD_TABLE_ID}",
        if_exists='append',
        location=LOCATION)

    report_region_data = get_async_report(report_region,)
    report_audience_data = get_async_report(report_audience,)

    df_audience = report_to_df(report_audience_data)
    df_region = report_to_df(report_region_data)


    if exist_dataset_table(client, PROJECT_ID, DATASET_ID, AUDIENCE_TABLE_ID, schema_facebook_audience, clustering_fields_audience, location=LOCATION, partition_field_name='date_start') == 'ok':
        df_audience.to_gbq(
        project_id=PROJECT_ID,
        destination_table=f"{DATASET_ID}.{AUDIENCE_TABLE_ID}",
        if_exists='append',
        location=LOCATION)

    if exist_dataset_table(client, PROJECT_ID, DATASET_ID, REGION_TABLE_ID, schema_facebook_region, clustering_fields_region, location=LOCATION, partition_field_name='date_start') == 'ok':
        df_region.to_gbq(
        project_id=PROJECT_ID,
        destination_table=f"{DATASET_ID}.{REGION_TABLE_ID}",
        if_exists='append',
        location=LOCATION)
