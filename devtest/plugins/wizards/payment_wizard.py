import random
from datetime import date, timedelta
from coframe.endpoints import endpoint
from coframe.memoryset import MemorySet


_CUSTOMERS = [
    ("Acme Srl",        "C001", "Mario Rossi",      "info@acme.it",               "+39 02 1234567",   "Via Roma 1, Milan, Italy"),
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
    Multi-step wizard — payment reminders.

    step=preview  build overdue rows via MemorySet → return data + schema
    step=confirm  reconstruct MemorySet from client rows → process selected
    """
    step = data.get('step')

    # ── Step 1: build preview ─────────────────────────────────────────────────
    if step == 'preview':
        min_amount = float(data.get('min_amount', 0) or 0)
        ms = MemorySet.from_yaml('payment_wizard_data')

        random.seed(42)
        today = date.today()

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
            credit_limit  = round(random.choice([5000, 10000, 20000, 50000]), 2)
            risk          = "High" if balance > credit_limit * 0.8 else ("Medium" if balance > credit_limit * 0.4 else "Low")

            if balance < min_amount:
                continue

            row = ms.add()
            row['id']            = i + 1
            row['code']          = code
            row['customer']      = name
            row['contact']       = contact
            row['agent']         = agent
            row['address']       = address
            row['invoice_no']    = f"FT{2025000 + i + 1}"
            row['invoice_date']  = invoice_date.isoformat()
            row['due_date']      = due_date.isoformat()
            row['overdue_days']  = overdue_days
            row['payment_terms'] = random.choice([30, 60, 90])
            row['amount']        = amount
            row['paid']          = paid
            row['balance']       = balance
            row['credit_limit']  = credit_limit
            row['open_orders']   = open_orders
            row['order_value']   = order_value
            row['risk']          = risk
            row['email']         = email
            row['phone']         = phone
            row['note']          = "Instalment plan in progress" if i == 2 else ""

        ms.sort('balance', ascending=False)

        return {
            'status':    'success',
            'data':      ms.to_list(),
            'schema':    ms.to_schema(),
            'next_step': 'confirm',
            'code':      200,
        }

    # ── Step 2: process confirmed rows ────────────────────────────────────────
    elif step == 'confirm':
        ms  = MemorySet.from_list(data.get('rows', []), 'payment_wizard_data')
        sel = ms.selected()
        if len(sel) == 0:
            sel = ms

        count     = len(sel)
        total     = sum(float(r.get('balance') or 0) for r in sel)
        high_risk = sum(1 for r in sel if r.get('risk') == 'High')

        return {
            'status': 'success',
            'data': {
                'processed': count,
                'total':     round(total, 2),
                'high_risk': high_risk,
                'message': (
                    f'{count} reminders sent — '
                    f'total outstanding: € {total:,.2f} '
                    f'({high_risk} high-risk customers)'
                ),
            },
            'next_step': 'done',
            'code':      200,
        }

    return {'status': 'error', 'message': f'Unknown step: {step!r}', 'code': 400}
