import pandas as pd
from config import WORD_SIGNALS, API_SIGNAL_RULES, BUSINESS_CONSTRAINTS, BQ_APPOINTMENT_RULES
from utils.logger import Logger

logger = Logger(__name__)


def analyze_api_signals(row):
    """
    Analyze API flags and determine boolean signals based on business rules.
    
    Args:
        row: Service type row with API flags
        
    Returns:
        dict: API signal analysis results
    """
    logger.debug(f"Analyzing API signals for TYPE_ID: {row['TYPE_ID']}")
    
    # Extract API flags with defaults
    api_frequency = row.get("API_FREQUENCY", 0) or 0
    api_reservice = row.get("API_RESERVICE", 0) or 0
    api_default_length = row.get("API_DEFAULT_LENGTH", 0) or 0
    api_regular_service = row.get("API_REGULAR_SERVICE", 0) or 0
    api_initial_id = row.get("API_INITIAL_ID", 0) or 0
    api_initial = row.get("API_INITIAL", 0) or 0
    
    # Apply business rules from config
    api_signals = {
        "isRecurring": False,
        "has_reservice": False,
        "isRervice": False,
        "zeroVisitTime": False
    }
    
    # FREQUENCY rules
    if API_SIGNAL_RULES["FREQUENCY"]["isRecurring"] is not None:
        result = API_SIGNAL_RULES["FREQUENCY"]["isRecurring"](api_frequency)
        if result is not None:
            api_signals["isRecurring"] = result
    if API_SIGNAL_RULES["FREQUENCY"]["isRervice"] is not None:
        result = API_SIGNAL_RULES["FREQUENCY"]["isRervice"](api_frequency)
        if result is not None:
            api_signals["isRervice"] = result
    
    # RESERVICE rules
    if API_SIGNAL_RULES["RESERVICE"]["isRecurring"] is not None:
        result = API_SIGNAL_RULES["RESERVICE"]["isRecurring"](api_reservice)
        if result is not None:
            api_signals["isRecurring"] = result
    if API_SIGNAL_RULES["RESERVICE"]["has_reservice"] is not None:
        result = API_SIGNAL_RULES["RESERVICE"]["has_reservice"](api_reservice)
        if result is not None:
            api_signals["has_reservice"] = result
    if API_SIGNAL_RULES["RESERVICE"]["isRervice"] is not None:
        result = API_SIGNAL_RULES["RESERVICE"]["isRervice"](api_reservice)
        if result is not None:
            api_signals["isRervice"] = result
    
    # DEFAULT_LENGTH rules
    if API_SIGNAL_RULES["DEFAULT_LENGTH"]["zeroVisitTime"] is not None:
        result = API_SIGNAL_RULES["DEFAULT_LENGTH"]["zeroVisitTime"](api_default_length)
        if result is not None:
            api_signals["zeroVisitTime"] = result
    
    # REGULAR_SERVICE rules
    if API_SIGNAL_RULES["REGULAR_SERVICE"]["isRecurring"] is not None:
        result = API_SIGNAL_RULES["REGULAR_SERVICE"]["isRecurring"](api_regular_service)
        if result is not None:
            api_signals["isRecurring"] = result
    if API_SIGNAL_RULES["REGULAR_SERVICE"]["has_reservice"] is not None:
        result = API_SIGNAL_RULES["REGULAR_SERVICE"]["has_reservice"](api_regular_service)
        if result is not None:
            api_signals["has_reservice"] = result
    
    # INITIAL_ID rules
    if API_SIGNAL_RULES["INITIAL_ID"]["isRecurring"] is not None:
        result = API_SIGNAL_RULES["INITIAL_ID"]["isRecurring"](api_initial_id)
        if result is not None:
            api_signals["isRecurring"] = result
    
    # INITIAL rules
    if API_SIGNAL_RULES["INITIAL"]["isRecurring"] is not None:
        result = API_SIGNAL_RULES["INITIAL"]["isRecurring"](api_initial)
        if result is not None:
            api_signals["isRecurring"] = result
    if API_SIGNAL_RULES["INITIAL"]["isRervice"] is not None:
        result = API_SIGNAL_RULES["INITIAL"]["isRervice"](api_initial)
        if result is not None:
            api_signals["isRervice"] = result
    if API_SIGNAL_RULES["INITIAL"]["zeroVisitTime"] is not None:
        result = API_SIGNAL_RULES["INITIAL"]["zeroVisitTime"](api_initial)
        if result is not None:
            api_signals["zeroVisitTime"] = result
    
    logger.debug(f"API signals for TYPE_ID {row['TYPE_ID']}: {api_signals}")
    
    return {
        "api_frequency": api_frequency,
        "api_reservice": api_reservice,
        "api_default_length": api_default_length,
        "api_regular_service": api_regular_service,
        "api_initial_id": api_initial_id,
        "api_initial": api_initial,
        **api_signals
    }


def analyze_text_signals(description, lookup_recurring=None):
    """
    Analyze text description for keyword-based signals.
    Word signals only indicate "True" - if no signal detected, it's None.
    
    Args:
        description: Service type description
        lookup_recurring: Optional lookup table recurring value
        
    Returns:
        dict: Text signal analysis results (True or None for each signal)
    """
    desc = description or ""
    desc_lower = desc.lower()
    
    logger.debug(f"Analyzing text signals for description: '{desc[:50]}...'")
    
    # Apply keyword-based detection - only True if keyword found, otherwise None
    word_signals = {
        "reservice": True if any(kw in desc_lower for kw in WORD_SIGNALS["reservice"]) else None,
        "recurring": True if any(kw in desc_lower for kw in WORD_SIGNALS["recurring"]) else None,
        "zero_time": True if any(kw in desc_lower for kw in WORD_SIGNALS["zero_time"]) else None,
        "has_reservice": True if any(kw in desc_lower for kw in WORD_SIGNALS["has_reservice"]) else None
    }
    
    # Do not override with lookup here; handled separately as SalesMapping source
    
    logger.debug(f"Text signals: {word_signals}")
    
    return word_signals


def resolve_final_signals(api_signals, word_signals):
    """
    Priority-based resolution with API and Word only is no longer used.
    Kept for backward compatibility but not called.
    """
    raise NotImplementedError("Legacy resolver is replaced by priority-based resolver")


def _pick_first_non_none(sources):
    for name, val, why in sources:
        if val is not None:
            return name, val, why
    return None, None, ""


def resolve_with_priorities(metric_name, sources_dict, priorities):
    """
    Resolve a metric using a list of source names in priority order.

    sources_dict: {
        'API': (val, reason),
        'Word': (val, reason),
        'SalesMapping': (val, reason),
        'Appointments': (val, reason),
        'BusinessRules': (val, reason),
    }
    priorities: ['SalesMapping', 'Appointments', 'API', 'Word', 'BusinessRules']

    Returns: chosen_value, chosen_source, chosen_reason, dissenting_reasons(list)
    """
    ordered = [(name, *sources_dict.get(name, (None, ""))) for name in priorities]
    chosen_source, chosen_val, chosen_reason = _pick_first_non_none(ordered)

    dissent = []
    if chosen_source is not None:
        # consider only sources ranked at or below the second-highest available for AskClient later
        for name, val, why in ordered:
            if name == chosen_source:
                continue
            if val is not None and val != chosen_val:
                dissent.append(f"{name} disagrees: {why}")

    return chosen_val, chosen_source, chosen_reason, dissent


def analyze_usage_patterns(type_id, appointments_df, subscriptions_df, service_types_df, client_id, cutoff_date):
    """
    Analyze usage patterns for a service type.
    
    Args:
        type_id: Service type ID
        appointments_df: Appointments dataframe
        subscriptions_df: Subscriptions dataframe
        service_types_df: Service types dataframe
        client_id: Client ID
        cutoff_date: Cutoff date for recent activity
        
    Returns:
        dict: Usage pattern analysis results
    """
    logger.debug(f"Analyzing usage patterns for TYPE_ID: {type_id}, Client: {client_id}")
    
    # Check recent appointments (past 2 years)
    relevant_appts = appointments_df[
        (appointments_df['clientID'] == client_id) &
        (appointments_df['type'] == type_id)
    ]
    recent_appts = relevant_appts[relevant_appts['appointmentDate'] >= cutoff_date]
    has_visits_past_2yrs = len(recent_appts) > 0
    
    # Check active subscriptions
    relevant_subs = subscriptions_df[subscriptions_df['clientID'] == client_id]
    relevant_subs = relevant_subs[
        (relevant_subs['serviceID'] == type_id) |
        (relevant_subs['serviceID'] == str(type_id))
    ]
    active_subs = relevant_subs[
        (relevant_subs['active'] == True) &
        (relevant_subs['dateCancelled'].isnull())
    ]
    has_active_subscription = len(active_subs) > 0
    
    # Check for repeated names
    try:
        current_description = service_types_df.loc[service_types_df['TYPE_ID'] == type_id, 'DESCRIPTION'].iloc[0]
        repeated_name = (service_types_df['DESCRIPTION'] == current_description).sum() > 1
    except (IndexError, KeyError):
        # If TYPE_ID not found or no description, assume not repeated
        repeated_name = False
    
    logger.debug(f"Usage patterns for TYPE_ID {type_id}: visits_past_2yrs={has_visits_past_2yrs}, active_subscription={has_active_subscription}, repeated_name={repeated_name}")
    
    return {
        "has_visits_past_2yrs": has_visits_past_2yrs,
        "has_active_subscription": has_active_subscription,
        "repeated_name": repeated_name,
        # Original logic: expired if NO visits in past 2 yrs OR NO active subscription
        "expired_code": (not has_visits_past_2yrs) or (not has_active_subscription)
    }


def analyze_appointment_recurring(type_id, appointments_df, subscriptions_df, client_id):
    """
    Derive appointment-based recurring signal for a service type within a client.

    Evidence considered (per individual account for this type):
    - Year-over-year presence (consecutive years)
    - Stable cadence based on median inter-visit days falling into configured bands

    Aggregation across accounts:
    - Compute ratio of accounts with strong evidence
    - Optionally boost with active subscription presence

    Returns:
        dict with keys:
            appt_recurring_bool: True | None
            appt_recurring_score: float in [0,1]
            appt_recurring_reason: str
    """
    logger.debug(f"Analyzing appointment-based recurring for TYPE_ID: {type_id}, Client: {client_id}")

    # Filter appointments for client and type, being tolerant of dtype mismatches
    appts_client = appointments_df[appointments_df['clientID'] == client_id].copy()
    if appts_client.empty:
        return {"appt_recurring_bool": None, "appt_recurring_score": 0.0, "appt_recurring_reason": "No appointments for client"}

    type_id_str = str(type_id)
    # Prefer fast string comparison to avoid dtype pitfalls
    appts_client['type_str'] = appts_client['type'].astype(str)
    appts_type = appts_client[appts_client['type_str'] == type_id_str].copy()
    if appts_type.empty:
        return {"appt_recurring_bool": None, "appt_recurring_score": 0.0, "appt_recurring_reason": "No appointments for this service type"}

    # Ensure dates are datetime
    appts_type['appointmentDate'] = pd.to_datetime(appts_type['appointmentDate'], errors='coerce')
    appts_type = appts_type.dropna(subset=['appointmentDate'])
    if appts_type.empty:
        return {"appt_recurring_bool": None, "appt_recurring_score": 0.0, "appt_recurring_reason": "No valid appointment dates"}

    min_visits = BQ_APPOINTMENT_RULES.get("APPT_MIN_VISITS_STRONG", 3)
    cadence_bands = BQ_APPOINTMENT_RULES.get("CADENCE_BANDS", {})
    pop_ratio_strong = BQ_APPOINTMENT_RULES.get("POP_RATIO_STRONG", 0.6)

    # Active subscription presence for this type
    subs_type = subscriptions_df[subscriptions_df['clientID'] == client_id]
    subs_type = subs_type[(subs_type['serviceID'] == type_id) | (subs_type['serviceID'] == type_id_str)]
    subs_active = subs_type[(subs_type['active'] == True) & (subs_type['dateCancelled'].isnull())]
    has_active_subscription = len(subs_active) > 0

    # Compute per-account evidence
    strong_flags = []
    account_stats = []
    for account_id, g in appts_type.groupby('individualAccountID'):
        g = g.sort_values('appointmentDate')
        visits = len(g)
        years = g['appointmentDate'].dt.year.dropna().unique()
        years_set = set(years.tolist())
        # Consecutive years evidence
        has_consecutive_years = any(((y + 1) in years_set) for y in years_set)

        # Stable cadence evidence via median inter-visit delta
        deltas = g['appointmentDate'].diff().dt.days.dropna()
        median_delta = None
        within_band = False
        if not deltas.empty:
            median_delta = float(deltas.median())
            for low, high in cadence_bands.values():
                if median_delta >= low and median_delta < high:
                    within_band = True
                    break

        stable_cadence_strong = visits >= min_visits and within_band
        account_strong = bool(has_consecutive_years or stable_cadence_strong)
        strong_flags.append(account_strong)
        account_stats.append({
            'account_id': account_id,
            'visits': visits,
            'years_count': len(years_set),
            'has_consecutive_years': bool(has_consecutive_years),
            'median_delta_days': median_delta,
            'within_band': bool(within_band),
            'strong': account_strong,
        })

    total_accounts = len(strong_flags)
    strong_accounts = sum(1 for f in strong_flags if f)
    strong_ratio = (strong_accounts / total_accounts) if total_accounts > 0 else 0.0

    # Decide score and boolean
    reason_parts = [f"{strong_accounts}/{total_accounts} accounts strong ({strong_ratio:.0%})"]
    if has_active_subscription:
        reason_parts.append("active subscription present")

    if strong_ratio >= pop_ratio_strong and has_active_subscription:
        score = 1.0
        appt_bool = True
        reason_parts.append("meets strong ratio threshold with active subscription")
    elif strong_ratio >= pop_ratio_strong:
        score = 0.7
        appt_bool = True
        reason_parts.append("meets strong ratio threshold")
    elif strong_accounts > 0:
        score = 0.5
        appt_bool = None  # weak evidence only
        reason_parts.append("some accounts show recurring, below threshold")
    else:
        score = 0.0
        appt_bool = None
        reason_parts.append("no recurring evidence")

    reason = "; ".join(reason_parts)
    logger.debug(f"Appointment recurring analysis for TYPE_ID {type_id}: bool={appt_bool}, score={score}, reason={reason}")
    return {
        "appt_recurring_bool": appt_bool,
        "appt_recurring_score": score,
        "appt_recurring_reason": reason,
    }


def check_business_constraints(final_signals):
    """
    Check business logic constraints on final resolved signals and apply corrections.
    
    Args:
        final_signals: Dictionary of final resolved signals
        
    Returns:
        tuple: (corrected_signals, violations, corrections_applied)
    """
    violations = []
    corrections_applied = []
    corrected_signals = final_signals.copy()
    
    # Only check constraints if signals are resolved (not None)
    
    # 1.1 - if recurring=True then has_reservice=True
    if corrected_signals.get("recurring") is True:
        if corrected_signals.get("has_reservice") is not True:
            corrected_signals["has_reservice"] = True
            corrections_applied.append("Set has_reservice to True due to recurring=True")
    
    # 1.2 - anything that is zeroVisitTime = True cannot have has_reservice as True, set has_reservice to False
    if corrected_signals.get("zero_time") is True and corrected_signals.get("has_reservice") is True:
        corrected_signals["has_reservice"] = False
        corrections_applied.append("Set has_reservice to False due to zeroVisitTime=True")
        if "zeroVisitTime_has_reservice" in BUSINESS_CONSTRAINTS:
            violations.append(BUSINESS_CONSTRAINTS["zeroVisitTime_has_reservice"])
    
    # Rule removed: zeroVisitTime=True no longer forces isRecurring=False
    
    # 1.3 - anything that is isRecurring = True cannot have isRervice as True, set isRervice to False
    if corrected_signals.get("recurring") is True and corrected_signals.get("reservice") is True:
        corrected_signals["reservice"] = False
        corrections_applied.append("Set isRervice to False due to isRecurring=True")
        violations.append(BUSINESS_CONSTRAINTS["isRecurring_isRervice"])

    # 1.4 - anything that is isRervice = True cannot have has_reservice = True, set has_reservice to False
    if corrected_signals.get("reservice") is True and corrected_signals.get("has_reservice") is True:
        corrected_signals["has_reservice"] = False
        corrections_applied.append("Set has_reservice to False due to isRervice=True")
        # Guard in case config is missing the key
        if "isRervice_hasReservice" in BUSINESS_CONSTRAINTS:
            violations.append(BUSINESS_CONSTRAINTS["isRervice_hasReservice"])
    
    if violations:
        logger.warning(f"Business constraint violations detected and corrected: {violations}")
        logger.info(f"Corrections applied: {corrections_applied}")
    
    return corrected_signals, violations, corrections_applied


def analyze_service_type(row, appointments_df, subscriptions_df, service_types_df, now, client_id, appt_share_pct_by_type=None, top20_type_ids=None, revenue_share_pct_by_type=None, top10_revenue_type_ids=None):
    """
    Main analysis function that orchestrates all analysis components.
    
    Args:
        row: Service type row
        appointments_df: Appointments dataframe
        subscriptions_df: Subscriptions dataframe
        service_types_df: Service types dataframe
        now: Current datetime
        client_id: Client ID
        
    Returns:
        dict: Complete analysis results
    """
    type_id = row["TYPE_ID"]
    desc = row["DESCRIPTION"] or ""
    lookup_recurring = row.get("isRecurring")
    
    logger.info(f"Starting analysis for TYPE_ID: {type_id}, Description: '{desc[:50]}...'")
    
    # Step 1: Analyze API signals
    api_analysis = analyze_api_signals(row)
    
    # Step 2: Analyze text signals (True or None only)
    word_analysis = analyze_text_signals(desc, lookup_recurring)
    
    # Step 3: Build source dictionaries and resolve by priorities
    # Sources per metric: API, Word, SalesMapping, Appointments; BusinessRules applied later
    sales_mapping_val = None
    sales_mapping_reason = ""
    if lookup_recurring is not None and str(lookup_recurring).strip() != "":
        normalized = str(lookup_recurring).strip().upper()
        if normalized in ("TRUE", "FALSE"):
            sales_mapping_val = True if normalized == "TRUE" else False
            sales_mapping_reason = f"SalesMapping={normalized}"

    appt_analysis = analyze_appointment_recurring(type_id, appointments_df, subscriptions_df, client_id)

    # Prepare per-metric sources
    sources_isRecurring = {
        'SalesMapping': (sales_mapping_val, sales_mapping_reason),
        'Appointments': (appt_analysis['appt_recurring_bool'], appt_analysis['appt_recurring_reason']),
        'API': (api_analysis['isRecurring'], f"API derived from flags: freq={api_analysis['api_frequency']}, reservice={api_analysis['api_reservice']}, initial={api_analysis['api_initial']}") ,
        'Word': (word_analysis['recurring'], 'Word keywords'),
        'BusinessRules': (None, ''),
    }
    sources_isRervice = {
        'API': (api_analysis['isRervice'], 'API RESERVICE rule'),
        'Word': (word_analysis['reservice'], 'Word keywords'),
        'BusinessRules': (None, ''),
    }
    sources_zeroTime = {
        'Word': (word_analysis['zero_time'], 'Word keywords'),
        'API': (api_analysis['zeroVisitTime'], 'API DEFAULT_LENGTH rule'),
        'BusinessRules': (None, ''),
    }
    # has_reservice primarily from business rules; API/Word are secondary
    # We'll compute after applying business rules.

    # Priority lists
    pr_isRecurring = ['SalesMapping', 'Appointments', 'API', 'Word']
    pr_isRervice = ['API', 'Word']
    pr_zeroTime = ['Word', 'API']

    chosen_recurring, src_recurring, why_recurring, dissent_recurring = resolve_with_priorities('isRecurring', sources_isRecurring, pr_isRecurring)
    chosen_reservice, src_reservice, why_reservice, dissent_reservice = resolve_with_priorities('isRervice', sources_isRervice, pr_isRervice)
    chosen_zero, src_zero, why_zero, dissent_zero = resolve_with_priorities('zeroVisitTime', sources_zeroTime, pr_zeroTime)

    # Build interim final_signals before business rules
    interim_signals = {
        'recurring': chosen_recurring,
        'reservice': chosen_reservice,
        'zero_time': chosen_zero,
        # has_reservice will be set by business rules below
        'has_reservice': api_analysis.get('has_reservice', api_analysis['isRecurring']) if chosen_recurring is None else None,
    }

    # Apply business constraints to derive has_reservice and enforce consistency
    corrected_signals, constraint_violations, corrections_applied = check_business_constraints(interim_signals)
    # Ensure has_reservice is not left as None
    if corrected_signals.get('has_reservice') is None:
        corrected_signals['has_reservice'] = False

    # AskClient rules: only two general rules
    askclient_reasons = []
    # Rule 1: Two highest-available sources disagree
    def top_two_disagree(priorities, sources):
        values = []
        for name in priorities:
            val, _ = sources.get(name, (None, ''))
            if val is not None:
                values.append((name, val))
            if len(values) == 2:
                break
        return len(values) == 2 and values[0][1] != values[1][1], values

    disagree, top_vals = top_two_disagree(pr_isRecurring, sources_isRecurring)
    if disagree:
        a, b = top_vals
        askclient_reasons.append(f"isRecurring: {a[0]}={a[1]} vs {b[0]}={b[1]}")
    disagree, top_vals = top_two_disagree(pr_isRervice, sources_isRervice)
    if disagree:
        a, b = top_vals
        askclient_reasons.append(f"isRervice: {a[0]}={a[1]} vs {b[0]}={b[1]}")
    disagree, top_vals = top_two_disagree(pr_zeroTime, sources_zeroTime)
    if disagree:
        a, b = top_vals
        askclient_reasons.append(f"zeroVisitTime: {a[0]}={a[1]} vs {b[0]}={b[1]}")

    # Rule 2: Highest available source says one thing, but every other source says otherwise
    def top_vs_rest(priorities, sources):
        vals = []
        for name in priorities:
            val, _ = sources.get(name, (None, ''))
            if val is not None:
                vals.append((name, val))
        if not vals:
            return False, []
        top_name, top_val = vals[0]
        rest = vals[1:]
        if rest and all(v != top_val for _, v in rest):
            return True, (top_name, top_val, [(n, v) for n, v in rest])
        return False, []

    flag, data = top_vs_rest(pr_isRecurring, sources_isRecurring)
    if flag:
        top_name, top_val, rest = data
        askclient_reasons.append(f"isRecurring: {top_name}={top_val} vs others={[f'{n}={v}' for n, v in rest]}")
    flag, data = top_vs_rest(pr_isRervice, sources_isRervice)
    if flag:
        top_name, top_val, rest = data
        askclient_reasons.append(f"isRervice: {top_name}={top_val} vs others={[f'{n}={v}' for n, v in rest]}")
    flag, data = top_vs_rest(pr_zeroTime, sources_zeroTime)
    if flag:
        top_name, top_val, rest = data
        askclient_reasons.append(f"zeroVisitTime: {top_name}={top_val} vs others={[f'{n}={v}' for n, v in rest]}")
    
    # High priority service: top 20 by appointment share per client
    appt_share_pct = None
    if appt_share_pct_by_type is not None:
        appt_share_pct = appt_share_pct_by_type.get(int(type_id), None)
    high_priority_reason = ""
    if top20_type_ids is not None and int(type_id) in top20_type_ids:
        high_priority_reason = "high priority service"
        askclient_reasons.append(high_priority_reason)

    # High revenue service: top 10 by revenue share per client (last 2 years)
    revenue_share_pct = None
    if revenue_share_pct_by_type is not None:
        revenue_share_pct = revenue_share_pct_by_type.get(int(type_id), None)
    high_revenue_reason = ""
    if top10_revenue_type_ids is not None and int(type_id) in top10_revenue_type_ids:
        high_revenue_reason = "high revenue service"
        askclient_reasons.append(high_revenue_reason)
    
    # Step 4: Analyze usage patterns
    cutoff_date = now - pd.DateOffset(years=2)
    usage_analysis = analyze_usage_patterns(type_id, appointments_df, subscriptions_df, service_types_df, client_id, cutoff_date)
    # Determine if client review is needed (only the two general rules)
    askclient = len(askclient_reasons) > 0
    
    logger.info(f"Analysis complete for TYPE_ID {type_id}: AskClient={askclient}, AskClient reasons={len(askclient_reasons)}, Constraint violations={len(constraint_violations)}")
    
    # Compile final results
    return {
        "TYPE_ID": type_id,
        "DESCRIPTION": desc,
        "API RESERVICE FLAG": api_analysis["api_reservice"],
        "API REGULAR_SERVICE FLAG": api_analysis["api_regular_service"],
        "API FREQUENCY FLAG": api_analysis["api_frequency"],
        "API DEFAULT_LENGTH FLAG": api_analysis["api_default_length"],
        "API INITIAL ID FLAG": api_analysis["api_initial_id"],
        "API INITIAL FLAG": api_analysis["api_initial"],
        "hasVisitsInPast2Years": usage_analysis["has_visits_past_2yrs"],
        "hasActiveSubscription": usage_analysis["has_active_subscription"],
        "Repeated Name": usage_analysis["repeated_name"],
        "API Reservice": api_analysis["isRervice"],
        "API Recurring": api_analysis["isRecurring"],
        "API Zero Time": api_analysis["zeroVisitTime"],
        "API Has Reservice": api_analysis["has_reservice"],
        "Word Signal Reservice": word_analysis["reservice"],
        "Word Signal Recurring": word_analysis["recurring"],
        "Word Signal Zero Time": word_analysis["zero_time"],
        "Word Signal Has Reservice": word_analysis["has_reservice"],
        "Appt Recurring": appt_analysis["appt_recurring_bool"],
        "Appt Recurring Score": appt_analysis["appt_recurring_score"],
        "Appt Recurring - Reason": appt_analysis["appt_recurring_reason"],
        "Final Reservice": corrected_signals.get("reservice"),
        "Final Recurring": corrected_signals.get("recurring"),
        "Final Zero Time": corrected_signals.get("zero_time"),
        "Final Has Reservice": corrected_signals.get("has_reservice"),
        "Expired Code": usage_analysis["expired_code"],
        "AskClient Reservice - Reason": "; ".join([r for r in askclient_reasons if r.startswith("isRervice:")]),
        "AskClient Recurring - Reason": "; ".join([r for r in askclient_reasons if r.startswith("isRecurring:")]),
        "AskClient Zero Time - Reason": "; ".join([r for r in askclient_reasons if r.startswith("zeroVisitTime:")]),
        "AskClient Has Reservice - Reason": "",
        "Appointment Share Pct": appt_share_pct,
        "Revenue Share Pct": revenue_share_pct,
        "AskClient High Priority - Reason": high_priority_reason,
        "AskClient High Revenue - Reason": high_revenue_reason,
        "AskClient": askclient,
        "Client": client_id
    }
