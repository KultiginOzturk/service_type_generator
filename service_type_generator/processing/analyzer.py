import pandas as pd
from config import WORD_SIGNALS, API_SIGNAL_RULES, BUSINESS_CONSTRAINTS
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
        "allocateReservices": False,
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
    if API_SIGNAL_RULES["RESERVICE"]["allocateReservices"] is not None:
        result = API_SIGNAL_RULES["RESERVICE"]["allocateReservices"](api_reservice)
        if result is not None:
            api_signals["allocateReservices"] = result
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
    if API_SIGNAL_RULES["REGULAR_SERVICE"]["allocateReservices"] is not None:
        result = API_SIGNAL_RULES["REGULAR_SERVICE"]["allocateReservices"](api_regular_service)
        if result is not None:
            api_signals["allocateReservices"] = result

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

    # Apply lookup table override for recurring
    if lookup_recurring:
        lookup_recurring = str(lookup_recurring).strip().upper()
        if lookup_recurring == "TRUE":
            word_signals["recurring"] = True
            logger.debug(f"Overriding recurring to True based on lookup table")
        elif lookup_recurring == "FALSE":
            word_signals["recurring"] = False  # This is an explicit False from lookup
            logger.debug(f"Overriding recurring to False based on lookup table")

    logger.debug(f"Text signals: {word_signals}")

    return word_signals


def resolve_final_signals(api_signals, word_signals):
    """
    Resolve final signals based on API and word signals.
    Final signal is True only if both API and word signals are True.
    If word signal is None, use API signal.
    If both have values but don't match, it's a conflict.

    Args:
        api_signals: Dictionary of API signals
        word_signals: Dictionary of word signals (True, False, or None)

    Returns:
        tuple: (final_signals, conflicts, has_conflicts)
    """
    logger.debug("Resolving final signals from API and word signals")

    final_signals = {}
    conflicts = {
        "reservice_reason": "",
        "recurring_reason": "",
        "zero_time_reason": "",
        "has_reservice_reason": ""
    }

    # Map API signals to word signals for comparison
    api_reservice = api_signals["isRervice"]
    api_recurring = api_signals["isRecurring"]
    api_zero_time = api_signals["zeroVisitTime"]
    api_has_reservice = api_signals["isRervice"] or api_signals["isRecurring"]

    # Resolve each signal
    # Reservice
    if word_signals["reservice"] is None:
        # No word signal, use API signal
        final_signals["reservice"] = api_reservice
    elif word_signals["reservice"] == api_reservice:
        # Both signals agree
        final_signals["reservice"] = api_reservice
    else:
        # Conflict - both have values but don't match
        final_signals["reservice"] = None  # Mark as unresolved
        conflicts[
            "reservice_reason"] = f"Conflict: API says reservice={api_reservice}, word signals={word_signals['reservice']}"

    # Recurring
    if word_signals["recurring"] is None:
        final_signals["recurring"] = api_recurring
    elif word_signals["recurring"] == api_recurring:
        final_signals["recurring"] = api_recurring
    else:
        final_signals["recurring"] = None
        conflicts[
            "recurring_reason"] = f"Conflict: API recurring={api_recurring}, word recurring={word_signals['recurring']}"

    # Zero Time
    if word_signals["zero_time"] is None:
        final_signals["zero_time"] = api_zero_time
    elif word_signals["zero_time"] == api_zero_time:
        final_signals["zero_time"] = api_zero_time
    else:
        final_signals["zero_time"] = None
        conflicts[
            "zero_time_reason"] = f"Conflict: API zeroTime={api_zero_time}, word zeroTime={word_signals['zero_time']}"

    # Has Reservice
    if word_signals["has_reservice"] is None:
        final_signals["has_reservice"] = api_has_reservice
    elif word_signals["has_reservice"] == api_has_reservice:
        final_signals["has_reservice"] = api_has_reservice
    else:
        final_signals["has_reservice"] = None
        conflicts[
            "has_reservice_reason"] = f"Conflict: API hasReservice={api_has_reservice}, word hasReservice={word_signals['has_reservice']}"

    # Check if any conflicts exist
    has_conflicts = any(conflicts.values())

    if has_conflicts:
        logger.info(f"Conflicts detected: {[k for k, v in conflicts.items() if v]}")

    logger.debug(f"Final signals: {final_signals}")

    return final_signals, conflicts, has_conflicts


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

    logger.debug(
        f"Usage patterns for TYPE_ID {type_id}: visits_past_2yrs={has_visits_past_2yrs}, active_subscription={has_active_subscription}, repeated_name={repeated_name}")

    return {
        "has_visits_past_2yrs": has_visits_past_2yrs,
        "has_active_subscription": has_active_subscription,
        "repeated_name": repeated_name,
        "expired_code": (not has_visits_past_2yrs) or (not has_active_subscription)
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

    # 1.1 - anything that is isRervice = True cannot have allocateReservices as True, set allocateReservices to False
    if corrected_signals.get("reservice") is True and corrected_signals.get("allocateReservices") is True:
        corrected_signals["allocateReservices"] = False
        corrections_applied.append("Set allocateReservices to False due to isRervice=True")
        violations.append(BUSINESS_CONSTRAINTS["isRervice_allocateReservices"])

    # 1.2 - anything that is zeroVisitTime = True cannot have allocateReservices as True, set allocateReservices to False
    if corrected_signals.get("zero_time") is True and corrected_signals.get("allocateReservices") is True:
        corrected_signals["allocateReservices"] = False
        corrections_applied.append("Set allocateReservices to False due to zeroVisitTime=True")
        violations.append(BUSINESS_CONSTRAINTS["zeroVisitTime_allocateReservices"])

    # Rule removed: zeroVisitTime=True no longer forces isRecurring=False

    # 1.4 - anything that is isRecurring = True cannot have isRervice as True, set isRervice to False
    if corrected_signals.get("recurring") is True and corrected_signals.get("reservice") is True:
        corrected_signals["reservice"] = False
        corrections_applied.append("Set isRervice to False due to isRecurring=True")
        violations.append(BUSINESS_CONSTRAINTS["isRecurring_isRervice"])

    if violations:
        logger.warning(f"Business constraint violations detected and corrected: {violations}")
        logger.info(f"Corrections applied: {corrections_applied}")

    return corrected_signals, violations, corrections_applied


def analyze_service_type(row, appointments_df, subscriptions_df, service_types_df, now, client_id):
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

    # Step 3: Resolve final signals and detect conflicts
    final_signals, conflicts, has_conflicts = resolve_final_signals(api_analysis, word_analysis)

    # Step 4: Analyze usage patterns
    cutoff_date = now - pd.DateOffset(years=2)
    usage_analysis = analyze_usage_patterns(type_id, appointments_df, subscriptions_df, service_types_df, client_id,
                                            cutoff_date)

    # Step 5: Check business constraints on final signals and apply corrections
    corrected_signals, constraint_violations, corrections_applied = check_business_constraints(final_signals)

    # Step 6: Determine if client review is needed
    askclient = has_conflicts or bool(constraint_violations)

    logger.info(
        f"Analysis complete for TYPE_ID {type_id}: AskClient={askclient}, Conflicts={has_conflicts}, Constraint violations={len(constraint_violations)}")

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
        "API Has Reservice": api_analysis["isRervice"] or api_analysis["isRecurring"],
        "Word Signal Reservice": word_analysis["reservice"],
        "Word Signal Recurring": word_analysis["recurring"],
        "Word Signal Zero Time": word_analysis["zero_time"],
        "Word Signal Has Reservice": word_analysis["has_reservice"],
        "Final Reservice": corrected_signals.get("reservice"),
        "Final Recurring": corrected_signals.get("recurring"),
        "Final Zero Time": corrected_signals.get("zero_time"),
        "Final Has Reservice": corrected_signals.get("has_reservice"),
        "Expired Code": usage_analysis["expired_code"],
        "AskClient Reservice - Reason": conflicts["reservice_reason"],
        "AskClient Recurring - Reason": conflicts["recurring_reason"],
        "AskClient Zero Time - Reason": conflicts["zero_time_reason"],
        "AskClient Has Reservice - Reason": conflicts["has_reservice_reason"],
        "AskClient": askclient,
        "Client": client_id
    }
