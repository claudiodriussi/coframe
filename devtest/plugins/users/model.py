from sqlalchemy.ext.hybrid import hybrid_property


class User:

    @hybrid_property
    def active_loans(self):
        """Restituisce i prestiti attivi dell'utente"""
        return [loan for loan in self.loans if not loan.returned_at]
