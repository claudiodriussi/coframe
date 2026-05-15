import random
from datetime import date, timedelta
from coframe.endpoints import endpoint


_CUSTOMERS = [
    ("Acme SRL",       "C001", "Mario Rossi",    "info@acme.it",             "+39 02 1234567",  "Via Roma 1, Milano"),
    ("Beta SpA",       "C002", "Luca Bianchi",   "amministrazione@beta.it",  "+39 011 9876543", "Corso Torino 22, Torino"),
    ("Gamma & Co",     "C003", "Anna Verdi",     "contabilita@gamma.it",     "+39 06 5554433",  "Via Veneto 5, Roma"),
    ("Delta Srl",      "C004", "Paolo Neri",     "dir@delta.eu",             "+39 051 3334422", "Via Marconi 8, Bologna"),
    ("Epsilon Corp",   "C005", "Sara Gialli",    "ufficio@epsilon.it",       "+39 049 7778899", "Via Università 3, Padova"),
    ("Zeta Trading",   "C006", "Giorgio Blu",    "zeta@zetatrading.com",     "+39 055 2223344", "Lungarno 14, Firenze"),
    ("Eta Servizi",    "C007", "Marta Rosa",     "info@etaservizi.it",       "+39 080 6667788", "Via Sparano 10, Bari"),
    ("Theta Group",    "C008", "Carlo Viola",    "billing@thetagroup.eu",    "+39 090 1112233", "Via Messina 7, Palermo"),
]

_AGENTS = ["Nord", "Centro", "Sud", "Export"]


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
            risk          = "Alto" if balance > credit_limit * 0.8 else ("Medio" if balance > credit_limit * 0.4 else "Basso")
            note          = "Accordo rateale in corso" if i == 2 else ""

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
        high_risk = sum(1 for r in rows if r.get('risk') == 'Alto')

        return {
            'status': 'success',
            'data': {
                'processed':  count,
                'total':      round(total, 2),
                'high_risk':  high_risk,
                'message':    (
                    f'{count} solleciti elaborati — '
                    f'totale esposto: € {total:,.2f} '
                    f'({high_risk} clienti ad alto rischio)'
                ),
            },
            'next_step': 'done',
            'code':      200,
        }

    return {'status': 'error', 'message': f'step sconosciuto: {step!r}', 'code': 400}
