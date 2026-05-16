import random
from datetime import date, timedelta
from coframe.endpoints import endpoint


_CUSTOMERS = [
    ("Acme Srl",        "C001", "Mario Rossi",      "info@acme.it",              "+39 02 1234567",   "Via Roma 1, Milan, Italy"),
    ("Beta SpA",        "C002", "Luca Bianchi",      "admin@beta.it",             "+39 011 9876543",  "Corso Torino 22, Turin, Italy"),
    ("Müller GmbH",     "C003", "Hans Müller",       "buchhaltung@mueller.de",    "+49 89 3456789",   "Leopoldstraße 12, Munich, Germany"),
    ("Dupont & Fils",   "C004", "Claire Dupont",     "compta@dupont.fr",          "+33 1 4455 6677",  "Rue de Rivoli 45, Paris, France"),
    ("Iberian Trade",   "C005", "Carlos García",     "facturacion@iberian.es",    "+34 93 2223344",   "Passeig de Gràcia 80, Barcelona, Spain"),
    ("Nordic Supply",   "C006", "Erik Lindqvist",    "billing@nordicsupply.se",   "+46 8 7778899",    "Kungsgatan 34, Stockholm, Sweden"),
    ("Atlas Corp",      "C007", "Sophie Bernard",    "finance@atlascorp.be",      "+32 2 5556677",    "Avenue Louise 100, Brussels, Belgium"),
    ("Hellenic Trade",  "C008", "Nikos Papadopoulos","info@hellenic.gr",          "+30 21 0334455",   "Ermou 18, Athens, Greece"),
]

_AGENTS = ["North", "Central", "South", "Export"]


@endpoint('payment_wizard')
def payment_wizard(data: dict) -> dict:
    """
    Multi-step wizard endpoint — payment reminders.

    step='preview'  receive params → return fake overdue rows + next_step
    step='confirm'  receive rows   → simulate processing → return summary
    """
    step = data.get('step')

    # ── Step 1: generate preview rows ─────────────────────────────────────────
    if step == 'preview':
        min_amount = float(data.get('min_amount', 0) or 0)

        today = date.today()
        rows = []
        random.seed(42)

        for i, (name, code, contact, email, phone, address) in enumerate(_CUSTOMERS):
            overdue_days  = random.randint(15, 180)
            due_date      = today - timedelta(days=overdue_days)
            invoice_date  = due_date - timedelta(days=random.randint(30, 90))
            amount        = round(random.uniform(200, 8000), 2)
            paid          = round(amount * random.uniform(0, 0.4), 2)
            balance       = round(amount - paid, 2)
            open_orders   = random.randint(0, 5)
            order_value   = round(random.uniform(0, 3000), 2) if open_orders else 0.0
            agent         = random.choice(_AGENTS)
            last_invoice  = f"FT{2025000 + i + 1}"
            payment_terms = random.choice([30, 60, 90])
            credit_limit  = round(random.choice([5000, 10000, 20000, 50000]), 2)
            risk          = "High" if balance > credit_limit * 0.8 else ("Medium" if balance > credit_limit * 0.4 else "Low")
            note          = "Instalment plan in progress" if i == 2 else ""

            if balance < min_amount:
                continue

            rows.append({
                'id':            i + 1,
                'code':          code,
                'customer':      name,
                'contact':       contact,
                'agent':         agent,
                'address':       address,
                'invoice_no':    last_invoice,
                'invoice_date':  invoice_date.isoformat(),
                'due_date':      due_date.isoformat(),
                'overdue_days':  overdue_days,
                'payment_terms': payment_terms,
                'amount':        amount,
                'paid':          paid,
                'balance':       balance,
                'credit_limit':  credit_limit,
                'open_orders':   open_orders,
                'order_value':   order_value,
                'risk':          risk,
                'email':         email,
                'phone':         phone,
                'note':          note,
            })

        return {
            'status':    'success',
            'data':      rows,
            'next_step': 'confirm',
            'code':      200,
        }

    # ── Step 2: process confirmed rows ────────────────────────────────────────
    elif step == 'confirm':
        rows    = data.get('rows', [])
        count   = len(rows)
        total   = sum(float(r.get('balance', 0)) for r in rows)
        high_risk = sum(1 for r in rows if r.get('risk') == 'High')

        return {
            'status': 'success',
            'data': {
                'processed':  count,
                'total':      round(total, 2),
                'high_risk':  high_risk,
                'message':    (
                    f'{count} reminders sent — '
                    f'total outstanding: € {total:,.2f} '
                    f'({high_risk} high-risk customers)'
                ),
            },
            'next_step': 'done',
            'code':      200,
        }

    return {'status': 'error', 'message': f'Unknown step: {step!r}', 'code': 400}
