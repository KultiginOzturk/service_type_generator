import pandas as pd

def analyze_service_type(row, appointments_df, subs_df, service_types_df, now, client_id):
    type_id = row["TYPE_ID"]
    desc = row["DESCRIPTION"] or ""

    api_reservice_flag = row["API_RESERVICE"] or 0
    api_regular_service_flag = row["API_REGULAR_SERVICE"] or 0
    api_frequency_flag = row["API_FREQUENCY"] or 0
    api_default_length_flag = row["API_DEFAULT_LENGTH"] or 0

    api_reservice_bool = (api_reservice_flag == 1)
    api_recurring_bool = (api_regular_service_flag == 1) and (api_frequency_flag > 0)
    api_zero_time_bool = (api_default_length_flag == 0)

    desc_lower = desc.lower()

    word_reservice_bool = any(kw in desc_lower for kw in [
        "reservice", "call back", "callback", "qc", "quality control", "quality"
    ])
    word_recurring_bool = any(kw in desc_lower for kw in [
        "recurring", "monthly", "bi-weekly", "weekly"
    ])

    # Override recurring signal if lookup table provides a value
    lookup_recurring = str(row.get("isRecurring", "")).strip().upper()
    if lookup_recurring == "TRUE":
        word_recurring_bool = True
    elif lookup_recurring == "FALSE":
        word_recurring_bool = False
    word_zero_time_bool = any(kw in desc_lower for kw in [
        "equipment", "charge", "lead", "donation", "cancellation", "fee", "write off", "write-off"
    ])
    word_has_reservice_bool = any(kw in desc_lower for kw in [
        "bed bug", "carpenter", "roach", "ant", "ants", "cockroach", "termite"
    ])

    cutoff_date = now - pd.DateOffset(years=2)
    relevant_appts = appointments_df[
        (appointments_df['clientID'] == client_id) &
        (appointments_df['type'] == type_id)
    ]
    recent_appts = relevant_appts[relevant_appts['appointmentDate'] >= cutoff_date]
    has_visits_past_2yrs = len(recent_appts) > 0

    relevant_subs = subs_df[subs_df['clientID'] == client_id]
    relevant_subs = relevant_subs[
        (relevant_subs['serviceID'] == type_id) |
        (relevant_subs['serviceID'] == str(type_id))
    ]
    active_subs = relevant_subs[
        (relevant_subs['active'] == True) &
        (relevant_subs['dateCancelled'].isnull())
    ]
    has_active_subscription = len(active_subs) > 0

    repeated_name = (service_types_df['DESCRIPTION'] == desc).sum() > 1
    api_has_reservice_bool = api_reservice_bool or api_recurring_bool
    expired_code_bool = (not has_visits_past_2yrs) or (not has_active_subscription)

    askclient_reservice_reason = ""
    askclient_recurring_reason = ""
    askclient_zerotime_reason = ""
    askclient_hasreservice_reason = ""

    if api_reservice_bool != word_reservice_bool:
        askclient_reservice_reason = f"Conflict: API says reservice={api_reservice_bool}, word signals={word_reservice_bool}"

    if api_recurring_bool != word_recurring_bool:
        if lookup_recurring in ("TRUE", "FALSE"):
            askclient_recurring_reason = (
                f"Conflict: API recurring={api_recurring_bool}, lookup recurring={word_recurring_bool}"
            )
        else:
            askclient_recurring_reason = (
                f"Conflict: API recurring={api_recurring_bool}, word recurring={word_recurring_bool}"
            )

    if api_zero_time_bool != word_zero_time_bool:
        askclient_zerotime_reason = f"Conflict: API zeroTime={api_zero_time_bool}, word zeroTime={word_zero_time_bool}"

    if api_has_reservice_bool != word_has_reservice_bool:
        askclient_hasreservice_reason = f"Conflict: API hasReservice={api_has_reservice_bool}, word hasReservice={word_has_reservice_bool}"

    askclient_bool = any([
        askclient_reservice_reason,
        askclient_recurring_reason,
        askclient_zerotime_reason,
        askclient_hasreservice_reason
    ])

    return {
        "TYPE_ID": type_id,
        "DESCRIPTION": desc,
        "API RESERVICE FLAG": api_reservice_flag,
        "API REGULAR_SERVICE FLAG": api_regular_service_flag,
        "API FREQUENCY FLAG": api_frequency_flag,
        "API DEFAULT_LENGTH FLAG": api_default_length_flag,
        "hasVisitsInPast2Years": has_visits_past_2yrs,
        "hasActiveSubscription": has_active_subscription,
        "Repeated Name": repeated_name,
        "API Reservice": api_reservice_bool,
        "API Recurring": api_recurring_bool,
        "API Zero Time": api_zero_time_bool,
        "API Has Reservice": api_has_reservice_bool,
        "Word Signal Reservice": word_reservice_bool,
        "Word Signal Recurring": word_recurring_bool,
        "Word Signal Zero Time": word_zero_time_bool,
        "Word Signal Has Reservice": word_has_reservice_bool,
        "Expired Code": expired_code_bool,
        "AskClient Reservice - Reason": askclient_reservice_reason,
        "AskClient Recurring - Reason": askclient_recurring_reason,
        "AskClient Zero Time - Reason": askclient_zerotime_reason,
        "AskClient Has Reservice - Reason": askclient_hasreservice_reason,
        "AskClient": askclient_bool,
        "Client": client_id
    }
