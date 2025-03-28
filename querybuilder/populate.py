import datetime
from sqlalchemy.orm import sessionmaker
from model import create_northwind_db, get_models
from decimal import Decimal


def populate_sample_data(db_path):
    """
    Populate the Northwind database with sample data
    """

    engine = create_northwind_db(db_path)
    Session = sessionmaker(bind=engine)
    session = Session()

    models = get_models()

    try:
        categories = [
            models['Category'](category_id=1, category_name="Beverages",
                               description="Soft drinks, coffees, teas, beers, and ales"),
            models['Category'](category_id=2, category_name="Condiments",
                               description="Sweet and savory sauces, relishes, spreads, and seasonings"),
            models['Category'](category_id=3, category_name="Confections",
                               description="Desserts, candies, and sweet breads"),
            models['Category'](category_id=4, category_name="Dairy Products",
                               description="Cheeses"),
            models['Category'](category_id=5, category_name="Grains/Cereals",
                               description="Breads, crackers, pasta, and cereal")
        ]
        session.add_all(categories)

        suppliers = [
            models['Supplier'](supplier_id=1, company_name="Exotic Liquids",
                               contact_name="Charlotte Cooper", contact_title="Purchasing Manager",
                               address="49 Gilbert St.", city="London", postal_code="EC1 4SD",
                               country="UK", phone="(171) 555-2222"),
            models['Supplier'](supplier_id=2, company_name="New Orleans Cajun Delights",
                               contact_name="Shelley Burke", contact_title="Order Administrator",
                               address="P.O. Box 78934", city="New Orleans", region="LA",
                               postal_code="70117", country="USA", phone="(100) 555-4822"),
            models['Supplier'](supplier_id=3, company_name="Grandma Kelly's Homestead",
                               contact_name="Regina Murphy", contact_title="Sales Representative",
                               address="707 Oxford Rd.", city="Ann Arbor", region="MI",
                               postal_code="48104", country="USA", phone="(313) 555-5735")
        ]
        session.add_all(suppliers)

        products = [
            models['Product'](product_id=1, product_name="Chai", supplier_id=1, category_id=1,
                              quantity_per_unit="10 boxes x 20 bags", unit_price=Decimal("18.00"),
                              units_in_stock=39, units_on_order=0, reorder_level=10, discontinued=False),
            models['Product'](product_id=2, product_name="Chang", supplier_id=1, category_id=1,
                              quantity_per_unit="24 - 12 oz bottles", unit_price=Decimal("19.00"),
                              units_in_stock=17, units_on_order=40, reorder_level=25, discontinued=False),
            models['Product'](product_id=3, product_name="Aniseed Syrup", supplier_id=1, category_id=2,
                              quantity_per_unit="12 - 550 ml bottles", unit_price=Decimal("10.00"),
                              units_in_stock=13, units_on_order=70, reorder_level=25, discontinued=False),
            models['Product'](product_id=4, product_name="Chef Anton's Cajun Seasoning", supplier_id=2, category_id=2,
                              quantity_per_unit="48 - 6 oz jars", unit_price=Decimal("22.00"),
                              units_in_stock=53, units_on_order=0, reorder_level=0, discontinued=False),
            models['Product'](product_id=5, product_name="Grandma's Boysenberry Spread", supplier_id=3, category_id=2,
                              quantity_per_unit="12 - 8 oz jars", unit_price=Decimal("25.00"),
                              units_in_stock=120, units_on_order=0, reorder_level=25, discontinued=False)
        ]
        session.add_all(products)

        customers = [
            models['Customer'](customer_id="ALFKI", company_name="Alfreds Futterkiste",
                               contact_name="Maria Anders", contact_title="Sales Representative",
                               address="Obere Str. 57", city="Berlin", postal_code="12209",
                               country="Germany", phone="030-0074321"),
            models['Customer'](customer_id="ANATR", company_name="Ana Trujillo Emparedados y helados",
                               contact_name="Ana Trujillo", contact_title="Owner",
                               address="Avda. de la Constitución 2222", city="México D.F.",
                               postal_code="05021", country="Mexico", phone="(5) 555-4729"),
            models['Customer'](customer_id="ANTON", company_name="Antonio Moreno Taquería",
                               contact_name="Antonio Moreno", contact_title="Owner",
                               address="Mataderos 2312", city="México D.F.",
                               postal_code="05023", country="Mexico", phone="(5) 555-3932")
        ]
        session.add_all(customers)

        employees = [
            models['Employee'](employee_id=1, last_name="Davolio", first_name="Nancy",
                               title="Sales Representative", title_of_courtesy="Ms.",
                               birth_date=datetime.date(1968, 12, 8),
                               hire_date=datetime.date(1992, 5, 1),
                               address="507 - 20th Ave. E. Apt. 2A", city="Seattle",
                               region="WA", postal_code="98122", country="USA",
                               home_phone="(206) 555-9857", extension="5467"),
            models['Employee'](employee_id=2, last_name="Fuller", first_name="Andrew",
                               title="Vice President, Sales", title_of_courtesy="Dr.",
                               birth_date=datetime.date(1952, 2, 19),
                               hire_date=datetime.date(1992, 8, 14),
                               address="908 W. Capital Way", city="Tacoma",
                               region="WA", postal_code="98401", country="USA",
                               home_phone="(206) 555-9482", extension="3457"),
            models['Employee'](employee_id=3, last_name="Leverling", first_name="Janet",
                               title="Sales Representative", title_of_courtesy="Ms.",
                               birth_date=datetime.date(1963, 8, 30),
                               hire_date=datetime.date(1992, 4, 1),
                               address="722 Moss Bay Blvd.", city="Kirkland",
                               region="WA", postal_code="98033", country="USA",
                               home_phone="(206) 555-3412", extension="3355")
        ]
        employees[1].reports_to = 2  # Nancy reports to Andrew
        employees[2].reports_to = 2  # Janet riporta ad Andrew

        session.add_all(employees)

        shippers = [
            models['Shipper'](shipper_id=1, company_name="Speedy Express", phone="(503) 555-9831"),
            models['Shipper'](shipper_id=2, company_name="United Package", phone="(503) 555-3199"),
            models['Shipper'](shipper_id=3, company_name="Federal Shipping", phone="(503) 555-9931")
        ]
        session.add_all(shippers)

        orders = [
            models['Order'](order_id=10248, customer_id="ALFKI", employee_id=1,
                            order_date=datetime.datetime(1996, 7, 4),
                            required_date=datetime.datetime(1996, 8, 1),
                            shipped_date=datetime.datetime(1996, 7, 16),
                            ship_via=3, freight=Decimal("32.38"),
                            ship_name="Alfreds Futterkiste",
                            ship_address="Obere Str. 57", ship_city="Berlin",
                            ship_postal_code="12209", ship_country="Germany"),
            models['Order'](order_id=10249, customer_id="ANATR", employee_id=3,
                            order_date=datetime.datetime(1996, 7, 5),
                            required_date=datetime.datetime(1996, 8, 16),
                            shipped_date=datetime.datetime(1996, 7, 10),
                            ship_via=1, freight=Decimal("11.61"),
                            ship_name="Ana Trujillo Emparedados y helados",
                            ship_address="Avda. de la Constitución 2222",
                            ship_city="México D.F.", ship_postal_code="05021",
                            ship_country="Mexico"),
            models['Order'](order_id=10250, customer_id="ANTON", employee_id=2,
                            order_date=datetime.datetime(1996, 7, 8),
                            required_date=datetime.datetime(1996, 8, 5),
                            shipped_date=datetime.datetime(1996, 7, 12),
                            ship_via=2, freight=Decimal("65.83"),
                            ship_name="Antonio Moreno Taquería",
                            ship_address="Mataderos 2312",
                            ship_city="México D.F.", ship_postal_code="05023",
                            ship_country="Mexico")
        ]
        session.add_all(orders)

        order_details = [
            models['OrderDetail'](order_id=10248, product_id=1, unit_price=Decimal("14.00"),
                                  quantity=12, discount=0),
            models['OrderDetail'](order_id=10248, product_id=3, unit_price=Decimal("9.80"),
                                  quantity=10, discount=0),
            models['OrderDetail'](order_id=10249, product_id=2, unit_price=Decimal("15.20"),
                                  quantity=5, discount=0),
            models['OrderDetail'](order_id=10249, product_id=4, unit_price=Decimal("17.60"),
                                  quantity=9, discount=0),
            models['OrderDetail'](order_id=10250, product_id=5, unit_price=Decimal("16.80"),
                                  quantity=20, discount=0)
        ]
        session.add_all(order_details)

        session.commit()
        return True

    except Exception as e:
        session.rollback()
        print(f"Errore durante l'inserimento dei dati: {e}")
        return False

    finally:
        session.close()
