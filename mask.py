import re
import logging

# ── LOGGING ───────────────────────────────────────────
logger = logging.getLogger('mask')

# ── SENSITIVE PATTERNS ────────────────────────────────

# Company name variations
COMPANY_PATTERNS = [
    r'Aadhar\s+Housing\s+Finance(?:\s+Company)?(?:\s+Limited)?',
    r'Adhaar\s+Housing\s+Finance(?:\s+Company)?(?:\s+Limited)?',
    r'AADHAR\s+HOUSING\s+FINANCE',
    r'Aadhar\s+Housing',
    r'Adhaar\s+Housing',
    r'AHF\b',
]

# City and branch names
LOCATION_PATTERNS = [
    r'\b(?:Mumbai|Delhi|Hyderabad|Pune|Bangalore|Chennai|'
    r'Kolkata|Ahmedabad|Jaipur|Lucknow|Surat|Nagpur|'
    r'Indore|Bhopal|Patna|Vadodara|Coimbatore|Visakhapatnam|'
    r'Vijayawada|Warangal|Guntur|Nellore|Tirupati|Kurnool)\b',
]

# Loan amount patterns
AMOUNT_PATTERNS = [
    r'INR\s*[\d,]+(?:\s*(?:Lakhs?|Crores?|L|Cr))?',
    r'Rs\.?\s*[\d,]+(?:\s*(?:Lakhs?|Crores?|L|Cr))?',
    r'₹\s*[\d,]+(?:\s*(?:Lakhs?|Crores?|L|Cr))?',
    r'\b\d+\s*(?:Lakhs?|Crores?)\b',
    r'\b\d+\s*(?:L|Cr)\b',
]


def mask_names(text, found_names):
    """
    Mask full names (First Last pattern).
    found_names: list of known names found in this video
    to mask consistently throughout.
    """
    masked = text
    count  = 0

    # Mask known names from video catalog
    for name in found_names:
        if name and len(name) > 3:
            pattern = re.escape(name)
            new_text = re.sub(pattern, '[PERSON]', masked, flags=re.IGNORECASE)
            if new_text != masked:
                count += len(re.findall(pattern, masked, flags=re.IGNORECASE))
                masked = new_text

    # Also mask any remaining "First Last" patterns
    # (two capitalised words together)
    name_pattern = r'\b[A-Z][a-z]+\s+[A-Z][a-z]+\b'
    remaining = re.findall(name_pattern, masked)

    # Filter out non-name phrases
    skip_words = ['Housing Finance', 'Relationship Manager', 'Senior Executive',
                  'Branch Manager', 'Sales Manager', 'Every Person',
                  'Gas Bill', 'Electricity Bill', 'Rental Agreement',
                  'Bank Statement', 'PAN Card', 'Self Declaration']

    for match in remaining:
        if match not in skip_words:
            masked = masked.replace(match, '[PERSON]')
            count += 1

    return masked, count


def mask_company(text):
    masked = text
    count  = 0
    for pattern in COMPANY_PATTERNS:
        matches = re.findall(pattern, masked, flags=re.IGNORECASE)
        count  += len(matches)
        masked  = re.sub(pattern, '[COMPANY]', masked, flags=re.IGNORECASE)
    return masked, count


def mask_locations(text):
    masked = text
    count  = 0
    for pattern in LOCATION_PATTERNS:
        matches = re.findall(pattern, masked, flags=re.IGNORECASE)
        count  += len(matches)
        masked  = re.sub(pattern, '[LOCATION]', masked, flags=re.IGNORECASE)
    return masked, count


def mask_amounts(text):
    masked = text
    count  = 0
    for pattern in AMOUNT_PATTERNS:
        matches = re.findall(pattern, masked, flags=re.IGNORECASE)
        count  += len(matches)
        masked  = re.sub(pattern, '[AMOUNT]', masked, flags=re.IGNORECASE)
    return masked, count


def mask_text(text, creator_name=None):
    """
    Main masking function.
    Pass creator_name to ensure it gets masked consistently.
    Returns: (masked_text, summary_of_what_was_masked)
    """
    if not text or text.strip() == "No screen text found":
        return text, {}

    original = text

    # Build list of known names to mask
    known_names = []
    if creator_name:
        known_names.append(creator_name)
        # Also add first name and last name separately
        parts = creator_name.strip().split()
        if len(parts) >= 2:
            known_names.extend(parts)

    
# Apply all masks — company FIRST before names
    text, company_count   = mask_company(text)
    text, location_count  = mask_locations(text)
    text, names_count     = mask_names(text, known_names)

    summary = {
        "names_masked":     names_count,
        "company_masked":   company_count,
        "locations_masked": location_count,
        "total_masked":     names_count + company_count + location_count
    }

    logger.info(f"Masking complete — {summary}")

    return text, summary


# ── TEST ──────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
    )

    # Test with Abhishek's transcript
    test_text = """
    Hello everyone, my name is Abhishek Sahu and I joined 
    Aadhar Housing Finance in July 2023. This was my first 
    job in the housing industry. I am based in Hyderabad.
    The company had given me a base target of INR 30 Lakhs. 
    But I had made my own target of INR 50 Lakhs.
    I am a Relationship Manager at Aadhar Housing Finance Company.
    """

    print("ORIGINAL TEXT:")
    print("=" * 50)
    print(test_text)

    masked, summary = mask_text(test_text, creator_name="Abhishek Sahu")

    print("\nMASKED TEXT:")
    print("=" * 50)
    print(masked)

    print("\nSUMMARY:")
    print("=" * 50)
    for key, val in summary.items():
        print(f"  {key}: {val}")