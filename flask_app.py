from flask import Flask, request
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import config
import model
import orm
import repository
import services


orm.start_mappers()
get_session = sessionmaker(bind=create_engine(config.get_postgres_uri()))
app = Flask(__name__)


@app.route("/allocate", methods=["POST"])
def allocate_endpoint():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    line = model.OrderLine(
        request.json["orderid"],
        request.json["sku"],
        request.json["qty"],
    )

    try:
        batchref = services.allocate(line, repo, session)
    except (model.OutOfStock, services.InvalidSku) as e:
        return {"message": str(e)}, 400

    return {"batchref": batchref}, 201

@app.route("/deallocate", methods=["POST"])
def deallocate_endpoint():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)

    line = session.query(model.OrderLine).filter_by(
        orderid = request.json["orderid"],
        sku = request.json["sku"],
    ).one()

    services.deallocate(line, repo, session)

    return {}, 201

@app.route("/add-batch", methods=["POST"])
def add_batch_endpoint():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    batch_ref = request.json["ref"]

    services.add_batch(
        batch_ref, 
        request.json["sku"], 
        request.json["qty"], 
        request.json["eta"], 
        repo, 
        session,
    )

    return {"ref": batch_ref}, 201
