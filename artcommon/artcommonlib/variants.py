"""
Backward-compatible exports for product identifiers.
"""

from typing import TypeAlias

from artcommonlib.product_catalog import get_product_id_for_product
from artcommonlib.product_ids import ProductId

BuildVariant: TypeAlias = ProductId


def get_build_variant_for_product(product: str) -> ProductId:
    """
    Resolve a product using the historical build-variant API.

    This compatibility wrapper returns the canonical ProductId. New code
    should use get_product_id_for_product directly.

    Arg(s):
        product: Product name to resolve.
    Return Value(s):
        ProductId: Canonical product identifier.
    """
    return get_product_id_for_product(product)
