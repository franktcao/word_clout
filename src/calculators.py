import math

from src.validation import _is_positive_integer, _is_positive_less_than_one


class TermFrequencyCalculator:
    """
    Calculate term frequency (TF) using different strategies.

    See https://en.wikipedia.org/wiki/Tf%E2%80%93idf for more information.
    """

    def __init__(
        self, term_count: int, terms_in_document: int, max_term_count: int
    ) -> None:
        """
        :param term_count:
            Number of times a given term appears in a given document
        :param terms_in_document:
            Total number of terms in a given document
        :param max_term_count:
            Greatest term count in a given document
        """
        _is_positive_integer(term_count=term_count)
        _is_positive_integer(terms_in_document=terms_in_document)
        _is_positive_integer(max_term_count=max_term_count)

        self.term_count = term_count
        self.tot_term_count = terms_in_document
        self.max_term_count = max_term_count

    # Not covered: Simple calculation
    @property
    def binary(self) -> float:  # pragma: no cover
        return 1

    # Not covered: Simple calculation
    @property
    def raw_count(self) -> float:  # pragma: no cover
        return self.term_count

    # Not covered: Simple calculation
    @property
    def term_frequency(self) -> float:  # pragma: no cover
        return self.term_count / float(self.tot_term_count)

    # Not covered: Simple calculation
    @property
    def log_normalization(self) -> float:  # pragma: no cover
        return 1 + math.log(1 + self.term_count)

    # Not covered: Simple calculation
    @property
    def double_normalization_half(self) -> float:  # pragma: no cover
        return self.get_double_k_normalization(k=0.5)

    # Not covered: Simple calculation
    def get_double_k_normalization(self, k: float) -> float:  # pragma: no cover
        _is_positive_less_than_one(k=k)
        return (1 - k) * (self.term_count / self.max_term_count) + k


class IdfScale:
    LOG = "log"
    STANDARD = "standard"


class InverseDocumentFrequencyCalculator:
    """
    Calculate inverse document frequency (IDF) using different strategies.

    See https://en.wikipedia.org/wiki/Tf%E2%80%93idf for more information.
    """

    def __init__(
        self,
        documents_with_term: int,
        documents_in_corpus: int,
        max_doc_count: int,
        scale: IdfScale = IdfScale.LOG,
    ) -> None:
        """
        :param documents_with_term:
            Number of documents (from a corpus) where a given term appears in
        :param documents_in_corpus:
            Total number of documents in corpus
        :param max_doc_count:
            Document count of term that appears in the most documents
        :para scale:
            Scale IDF by logarithm or not. Options
        """
        _is_positive_integer(documents_with_term=documents_with_term)
        _is_positive_integer(documents_in_corpus=documents_in_corpus)
        _is_positive_integer(max_doc_count=max_doc_count)

        self.documents_with_term = documents_with_term
        self.documents_in_corpus = documents_in_corpus
        self.max_doc_count = max_doc_count
        self.scale = scale

    # Not covered: Simple calculation
    @property
    def unary(self) -> float:  # pragma: no cover
        return 1.0

    # Not covered: Simple calculation
    @property
    def document_frequency(self) -> float:  # pragma: no cover
        result = float(self.documents_in_corpus) / self.documents_with_term
        return self.rescale(result)

    # Not covered: Simple calculation
    @property
    def smooth(self) -> float:  # pragma: no cover
        result = self.documents_in_corpus / (1.0 + self.documents_with_term)
        extra = 1 if self.scale == IdfScale.LOG else 0
        return self.rescale(result) + extra

    # Not covered: Simple calculation
    @property
    def max(self) -> float:  # pragma: no cover
        result = self.max_doc_count / (1.0 + self.documents_with_term)
        return self.rescale(result)

    # Not covered: Simple calculation
    @property
    def probabilistic(self) -> float:  # pragma: no cover
        doc_count = self.documents_with_term
        result = float(self.documents_in_corpus - doc_count) / doc_count
        return self.rescale(result)

    # Not covered: Simple calculation
    def rescale(self, value) -> float:  # pragma: no cover
        """
        Return rescaled value depending on scale setting.

        :param value:
            Value to scale
        :return:
            Either value or rescaled value depending on `self.scale`
        """
        if self.scale == IdfScale.STANDARD:
            return value
        elif self.scale == IdfScale.LOG:
            return math.log(value)
