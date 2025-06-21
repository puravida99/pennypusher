""" """

import abc
import polars as pl
from typing import Optional, Union


class Transform(abc.ABC):
    """ """

    @abc.abstractmethod
    def apply(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        raise NotImplementedError


class DatetimeTransform(Transform):
    """ """

    def __init__(self, col: str, outcol: str, fmt: Optional[str] = "%Y-%m-%d"):
        super().__init__()
        self.col = col
        self.outcol = outcol
        self.fmt = fmt

    def apply(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_column(
            pl.col(self.col).str.strptime(pl.Datetime, self.fmt).alias(self.outcol)
        )


class StringCleaner(Transform):
    """
    Replaces all occurrences of a regex pattern in a string column.
    """

    def __init__(
        self, col: str, outcol: str, regex: str, replacement: Optional[str] = ""
    ):
        """
        Args:
            col (str): The column to clean.
            outcol (str): The column to store the cleaned string.
            regex (str): The regex pattern to replace.
            replacement (str, optional): The replacement string. Defaults to empty.
        """
        super().__init__()
        self.col = col
        self.outcol = outcol
        self.regex = regex
        self.replacement = replacement

    def apply(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_column(
            pl.col(self.col)
            .str.replace_all(self.regex, self.replacement)
            .alias(self.outcol)
        )


class AddColumn(Transform):
    """
    Creates a column from a literal (string or number)
    """

    def __init__(self, literal: Union[str, int, float], outcol: str):
        super.__init__()
        self.literal = literal
        self.outcol = outcol

    def apply(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_column(pl.lit(self.literal).alias(self.outcol))


class InferCurrency(Transform):
    """
    Infers currency from a string containing a monetary amount.
    """

    def __init__(self, amount_col: str, outcol: str):
        """
        Args:
            amount_col (str): The column containing the amount.
            outcol (str): The column to store the inferred currency.
        """
        super().__init__()
        self.amount_col = amount_col
        self.outcol = outcol

        # Build a polars when-then expression using the ISO 4217 currency codes...
        self.tmp_col = "currency_symbol"
        currency_map = {
            "$": "USD",
            "US": "USD",
            "€": "EUR",
            "£": "GBP",
            "¥": "JPY",
            "C$": "CAD",
            "CAD": "CAD",
            "A$": "AUD",
            "AUD": "AUD",
            "S$": "SGD",
            "SGD": "SGD",
            "NZ$": "NZD",
            "NZD": "NZD",
            "R$": "BRL",
            "BRL": "BRL",
            "BTC": "BTC",
        }
        self.currency_expr = None
        for k, v in currency_map.items():
            if self.currency_expr is None:
                self.currency_expr = pl.when(pl.col(self.tmp_col) == k)
            else:
                self.currency_expr = self.currency_expr.when(pl.col(self.tmp_col) == k)
            self.currency_expr = self.currency_expr.then(pl.lit(v))
        self.currency_expr = self.currency_expr.otherwise("UKN")

    def apply(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        lf = lf.with_column(
            pl.col(self.amount_col).extract(r"([^\d\s]+", group=1).alias(self.tmp_col)
        )
        lf = lf.with_column(self.currency_expr.alias(self.outcol))
        return lf.drop(self.tmp_col)


class CreditDebitToAmount(Transform):
    """
    Collapes credit and debit columns into a single column with a signed amount
    representing the change in balance for the given entry.
    """

    def __init__(self, credit_col: str, debit_col: str, outcol: str):
        """
        Args:
            credit_col (str): The column containing the credit amount.
            debit_col (str): The column containing the debit amount.
            outcol (str): The column to store the amount.
        """
        super().__init__()
        self.credit_col = credit_col
        self.debit_col = debit_col
        self.outcol = outcol

    def apply(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_column(
            (pl.col(self.credit_col) - pl.col(self.debit_col)).alias(self.outcol)
        )
