import abc
import model


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError


class SqlRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    def add(self, batch):
        eta = f'"{batch.eta}"' if batch.eta else "NULL"
        self.session.execute(f'INSERT INTO batches (reference,sku,eta,_purchased_quantity) VALUES ("{batch.reference}", "{batch.sku}", {eta}, {batch._purchased_quantity})')

        created_batch_id = self.session.execute(f'SELECT id from batches where reference= "{batch.reference}"').first()[0]

        for l in batch._allocations:
            self.session.execute(f'INSERT INTO order_lines (sku,qty,orderid) VALUES ("{l.sku}", {l.qty}, "{l.orderid}")')
            line_id = self.session.execute(f'SELECT id from order_lines where sku="{l.sku}" and qty={l.qty} and orderid="{l.orderid}"').first()[0]
            self.session.execute(f'INSERT INTO allocations (orderline_id,batch_id) VALUES ({line_id},{created_batch_id})')
        

    def get(self, reference) -> model.Batch:
        batch = self.session.execute(f'SELECT * from batches where reference= "{reference}"').first()
        lines = self.session.execute(f'SELECT * from order_lines where id in (SELECT orderline_id from allocations where batch_id= "{batch[0]}")')

        model_batch = model.Batch(
            ref=batch[1],
            sku=batch[2], 
            qty=batch[3],
            eta=batch[4],
        )

        for l in lines:
            model_batch.allocate(model.OrderLine(orderid=l[3], sku=l[1], qty=l[2]))

        return model_batch
