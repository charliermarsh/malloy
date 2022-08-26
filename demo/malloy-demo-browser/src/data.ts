import { Geography } from "./types";

export const GEOGRAPHY_TO_QUERY: { [K in Geography]: string } = {
  Georgia: "georgia_dashboard",
  Pennsylvania: "pennsylvania_dashboard",
};

export const MODEL = `source: political_ads is table('duckdb-wasm:https://sql-cached-artifact-api.anomander.xyz/artifacts/political-spend-big/31353222-8a65-4d62-87cb-e60415390413.parquet') {
  measure:
    total_spend is sum((spend_range_max_usd))
    percent_of_spend is total_spend/all(total_spend) * 100
    ad_count is count(distinct ad_id)

  query: top_ads is {
    group_by:
      start is date_range_start
      ad_type
      ad_url
    aggregate:
      total_spend
      percent_of_spend
    group_by: relevant_geos
    order_by: total_spend desc
  }

  query: spend_dashboard is {
    top: 20
    group_by: advertiser_name
    aggregate:
      spend_30_days is total_spend {
        where: date_range_start = now - 30 days for 30 days
      }
      lifetime_spend is total_spend
      lifetime_ad_count is ad_count
    nest: spend_over_time_line_chart is {
      group_by: start_week is date_range_start.week
      aggregate: total_spend
    }
    nest: by_media_type_bar_chart is {
      group_by: ad_type
      aggregate: total_spend, ad_count
      order_by: ad_type
    }
    nest: recent_ads is top_ads {
      where: date_range_start = now - 30 days for 30 days
      limit: 5
    }
    nest: top_lifetime_spend is top_ads {
      limit: 5
    }
  }
}

query: pennsylvania_dashboard is political_ads-> spend_dashboard {
  where: relevant_geos ~r'Pennsylvania'
}

query: georgia_dashboard is political_ads-> spend_dashboard {
  where: relevant_geos ~r'Georgia'
}`;
