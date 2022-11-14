from __future__ import annotations

import model
from model import OrderLine
from repository import AbstractRepository


class InvalidSku(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


def allocate(line: OrderLine, repo: AbstractRepository, session) -> str:
    batches = repo.list()
    if not is_valid_sku(line.sku, batches):
        raise InvalidSku(f"Invalid sku {line.sku}")
    batchref = model.allocate(line, batches)
    session.commit()
    return batchref

def deallocate(line: OrderLine, repo: AbstractRepository, session):
    batches = repo.list()
    if not is_valid_sku(line.sku, batches):
        raise InvalidSku(f"Invalid sku {line.sku}")
    for b in batches:
        if b.sku==line.sku:
            b.deallocate(line)
    session.commit()


def add_batch(
    ref: str, 
    sku: str, 
    qty: int, 
    eta: Optional[date], 
    repo: AbstractRepository, 
    session):
    repo.add(
        model.Batch(
            ref,sku,qty,eta
        )
    )
    session.commit()
