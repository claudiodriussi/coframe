from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Date, Text
from sqlalchemy import Boolean, ForeignKey, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import os

Base = declarative_base()


class Category(Base):
    __tablename__ = 'categories'

    category_id = Column(Integer, primary_key=True)
    category_name = Column(String(15), nullable=False)
    description = Column(Text)
    picture = Column(String)

    # Relationships
    products = relationship("Product", back_populates="category")

    def __repr__(self):
        return f"<Category(category_name='{self.category_name}')>"


class Customer(Base):
    __tablename__ = 'customers'

    customer_id = Column(String(5), primary_key=True)
    company_name = Column(String(40), nullable=False)
    contact_name = Column(String(30))
    contact_title = Column(String(30))
    address = Column(String(60))
    city = Column(String(15))
    region = Column(String(15))
    postal_code = Column(String(10))
    country = Column(String(15))
    phone = Column(String(24))
    fax = Column(String(24))

    # Relationships
    orders = relationship("Order", back_populates="customer")

    def __repr__(self):
        return f"<Customer(company_name='{self.company_name}')>"


class Employee(Base):
    __tablename__ = 'employees'

    employee_id = Column(Integer, primary_key=True)
    last_name = Column(String(20), nullable=False)
    first_name = Column(String(10), nullable=False)
    title = Column(String(30))
    title_of_courtesy = Column(String(25))
    birth_date = Column(Date)
    hire_date = Column(Date)
    address = Column(String(60))
    city = Column(String(15))
    region = Column(String(15))
    postal_code = Column(String(10))
    country = Column(String(15))
    home_phone = Column(String(24))
    extension = Column(String(4))
    photo = Column(String)
    notes = Column(Text)
    reports_to = Column(Integer, ForeignKey('employees.employee_id'))
    photo_path = Column(String(255))

    # Relationships
    orders = relationship("Order", back_populates="employee")
    reports_to_employee = relationship("Employee", remote_side=[employee_id])
    subordinates = relationship("Employee", back_populates="reports_to_employee")

    def __repr__(self):
        return f"<Employee(last_name='{self.last_name}', first_name='{self.first_name}')>"


class OrderDetail(Base):
    __tablename__ = 'order_details'

    order_id = Column(Integer, ForeignKey('orders.order_id'), primary_key=True)
    product_id = Column(Integer, ForeignKey('products.product_id'), primary_key=True)
    unit_price = Column(Numeric(10, 2), nullable=False)
    quantity = Column(Integer, nullable=False, default=1)
    discount = Column(Float, nullable=False, default=0)

    # Relationships
    order = relationship("Order", back_populates="order_details")
    product = relationship("Product", back_populates="order_details")

    def __repr__(self):
        return f"<OrderDetail(order_id={self.order_id}, product_id={self.product_id})>"


class Order(Base):
    __tablename__ = 'orders'

    order_id = Column(Integer, primary_key=True)
    customer_id = Column(String(5), ForeignKey('customers.customer_id'))
    employee_id = Column(Integer, ForeignKey('employees.employee_id'))
    order_date = Column(DateTime)
    required_date = Column(DateTime)
    shipped_date = Column(DateTime)
    ship_via = Column(Integer, ForeignKey('shippers.shipper_id'))
    freight = Column(Numeric(10, 2), default=0)
    ship_name = Column(String(40))
    ship_address = Column(String(60))
    ship_city = Column(String(15))
    ship_region = Column(String(15))
    ship_postal_code = Column(String(10))
    ship_country = Column(String(15))

    # Relationships
    customer = relationship("Customer", back_populates="orders")
    employee = relationship("Employee", back_populates="orders")
    shipper = relationship("Shipper", back_populates="orders")
    order_details = relationship("OrderDetail", back_populates="order", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Order(order_id={self.order_id}, order_date='{self.order_date}')>"


class Product(Base):
    __tablename__ = 'products'

    product_id = Column(Integer, primary_key=True)
    product_name = Column(String(40), nullable=False)
    supplier_id = Column(Integer, ForeignKey('suppliers.supplier_id'))
    category_id = Column(Integer, ForeignKey('categories.category_id'))
    quantity_per_unit = Column(String(20))
    unit_price = Column(Numeric(10, 2), default=0)
    units_in_stock = Column(Integer, default=0)
    units_on_order = Column(Integer, default=0)
    reorder_level = Column(Integer, default=0)
    discontinued = Column(Boolean, nullable=False, default=False)

    # Relationships
    supplier = relationship("Supplier", back_populates="products")
    category = relationship("Category", back_populates="products")
    order_details = relationship("OrderDetail", back_populates="product")

    def __repr__(self):
        return f"<Product(product_name='{self.product_name}')>"


class Shipper(Base):
    __tablename__ = 'shippers'

    shipper_id = Column(Integer, primary_key=True)
    company_name = Column(String(40), nullable=False)
    phone = Column(String(24))

    # Relationships
    orders = relationship("Order", back_populates="shipper")

    def __repr__(self):
        return f"<Shipper(company_name='{self.company_name}')>"


class Supplier(Base):
    __tablename__ = 'suppliers'

    supplier_id = Column(Integer, primary_key=True)
    company_name = Column(String(40), nullable=False)
    contact_name = Column(String(30))
    contact_title = Column(String(30))
    address = Column(String(60))
    city = Column(String(15))
    region = Column(String(15))
    postal_code = Column(String(10))
    country = Column(String(15))
    phone = Column(String(24))
    fax = Column(String(24))
    homepage = Column(Text)

    # Relationships
    products = relationship("Product", back_populates="supplier")

    def __repr__(self):
        return f"<Supplier(company_name='{self.company_name}')>"


def create_northwind_db(db_path="northwind.sqlite"):
    """
    Create a SQLite db with the Northwind schema.

    :param db_path: filename for db
    :return: the engine
    """
    if os.path.exists(db_path):
        os.remove(db_path)

    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)

    return engine


def northwind_engine(db_path="northwind.sqlite"):
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    return engine


def get_models():
    """
    Return a dict with all models in the database

    :return: dict of models
    """
    return {
        'Category': Category,
        'Customer': Customer,
        'Employee': Employee,
        'OrderDetail': OrderDetail,
        'Order': Order,
        'Product': Product,
        'Shipper': Shipper,
        'Supplier': Supplier
    }
