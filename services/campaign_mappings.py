

# Define campaign mapping
campaign_mapping = {
    "first_five_bills": {
        "free_otc": {
            "first": "FREE_OTC_v1_ENG",
            "second": {
                "hindi": "FREE_OTC_v1_HI",
                "gujarati": "FREE_OTC_v1_GU"
            }
        },
        "25_rupees": {
            "first": "OFFER_CAMPAIGN_AUTO_MKT",
            "second": {
                "hindi": "OFFER_CAMPAIGN_AUTO_MKT",
                "gujarati": "OFFER_CAMPAIGN_AUTO_MKT"
            }
        },
        "msp": {
            "first": "FIRST5_MSP_INVITE_R1_ENG",
            "second": {
                "hindi": "FIRST5_MSP_INVITE_R2_HI",
                "gujarati": "FIRST5_MSP_INVITE_R3_GU"
            }
        }
    },
    "repeat": {
        "branded_chronic": {
            "first": "GENERIC_REPLACEMENT_REMINDER_13",
            "second": {
                "hindi": "GENERIC_REPLACEMENT_REMINDER_13_HI_V2",
                "gujarati": "GENERIC_REPLACEMENT_REMINDER_13_GU_V3"
            }
        },
        "other": {
            "first": "REPEAT_OTH_R1_ENG",
            "second": {
                "hindi": "REPEAT_OTH_R2_HIN",
                "gujarati": "REPEAT_OTH_R2_GUJ"
            }
        },
        "msp": {
            "first": "REPEAT_MSP_R1_ENG_v2",
            "second": {
                "hindi": "REPEAT_MSP_R2_HIN_v1",
                "gujarati": "REPEAT_MSP_R2_GUJ_v1"
            }
        }
    },
    "lost": {
        "branded_chronic": {
            "first": "GENERIC_REPLACEMENT_REMINDER_13",
            "second": {
                "hindi": "GENERIC_REPLACEMENT_REMINDER_13_HI_V2",
                "gujarati": "GENERIC_REPLACEMENT_REMINDER_13_GU_V3"
            }
        },
        "free_otc": {
            "first": "FREE_OTC_v1_ENG",
            "second": {
                "hindi": "FREE_OTC_v1_HI",
                "gujarati": "FREE_OTC_v1_GU"
            }
        },
        "25_rupees": {
            "first": "OFFER_CAMPAIGN_AUTO_MKT",
            "second": {
                "hindi": "OFFER_CAMPAIGN_AUTO_MKT",
                "gujarati": "OFFER_CAMPAIGN_AUTO_MKT"
            }
        },
        "msp": {
            "first": "REPEAT_MSP_R1_ENG_v2",
            "second": {
                "hindi": "REPEAT_MSP_R2_HIN_v1",
                "gujarati": "REPEAT_MSP_R2_GUJ_v1"
            }
        },
        "17_percent": {
            "first": "REPEAT_MSP_R1_ENG_v2",
            "second": {
                "hindi": "REPEAT_MSP_R2_HIN_v1",
                "gujarati": "REPEAT_MSP_R2_GUJ_v1"
            }
        }
    }
}


def map_campaign(df, input_round):
    def get_campaign_name(row):
        campaign_type = row['campaign'].lower()
        sub_campaign = row['campaign_type'].lower()
        language = row['language'].lower()

        if campaign_type in campaign_mapping:
            mapping = campaign_mapping[campaign_type]
            if input_round == "first":
                return mapping.get(sub_campaign, {}).get('first', None)
            elif input_round == "second":
                return mapping.get(sub_campaign, {}).get('second', {}).get(language, None)
        return None

    df['campaign_name'] = df.apply(get_campaign_name, axis=1)
    return df
