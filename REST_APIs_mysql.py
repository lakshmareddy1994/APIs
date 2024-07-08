from flask import Flask, request, jsonify, render_template
from flask_sqlalchemy import SQLAlchemy
import os
from dotenv import load_dotenv
from sqlalchemy import update, MetaData, Table, inspect
import urllib.parse

class FlaskApp:
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()

        # Initialize Flask app
        self.app = Flask(__name__)

        # Configure the app
        self.configure_app()

        # Initialize SQLAlchemy
        self.db = SQLAlchemy(self.app)

        # Define routes
        self.define_routes()

    def configure_app(self):
        # Database credentials from environment variables
        server = os.getenv('MYSQL_DB_SERVER')
        database = os.getenv('MYSQL_DB_DATABASE')
        username = os.getenv('MYSQL_DB_USER')
        password = os.getenv('MYSQL_DB_PASSWORD')

        # Quote special characters in the username and password
        quoted_username = urllib.parse.quote_plus(username.encode('utf-8'))
        quoted_password = urllib.parse.quote_plus(password.encode('utf-8'))

        # Set up the SQLAlchemy part
        self.app.config['SQLALCHEMY_DATABASE_URI'] = f"mysql+pymysql://{quoted_username}:{quoted_password}@{server}/{database}?charset=utf8mb4"
        self.app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False


    def define_routes(self):
        @self.app.route('/read_table', methods=['POST'])
        def read_table():
            try:
                # Extract data from request payload
                data = request.get_json()
                if not data or 'table_name' not in data:
                    raise ValueError("Invalid JSON format. 'table' must be provided.")

                table_name = data['table_name']

                # Get list of schemas
                schemas_response, schemas_status = self.get_schemas()
                if schemas_status != 200:
                    return jsonify(schemas_response), schemas_status

                schemas = schemas_response['schemas']

                # Check if table exists in any schema
                schema_name = None
                for schema in schemas:
                    if self.table_exists_in_schema(schema, table_name):
                        schema_name = schema
                        break

                if not schema_name:
                    raise ValueError(f"Table '{table_name}' does not exist in any schema.")

                table = Table(table_name, MetaData(), schema=schema_name, autoload_with=self.db.engine)
                query = table.select()
                conn = self.db.engine.connect()
                result = conn.execute(query)
                rows = result.fetchall()
                columns = result.keys()
                conn.close()

                data = [dict(zip(columns, row)) for row in rows]

                return jsonify(data), 200
            except ValueError as ve:
                return jsonify({"error": str(ve)}), 400
            except Exception as e:
                return jsonify({"error": "Failed to read table", "details": str(e)}), 500

        @self.app.route('/add_record', methods=['POST'])
        def add_record():
            try:
                # Extract data from request payload
                data = request.get_json()
                if not data or 'table_name' not in data or 'table_data' not in data:
                    raise ValueError("Invalid JSON format. 'table_name' and 'table_data' must be provided.")

                table_name = data['table_name']
                table_data = data['table_data']

                # Get list of schemas
                schemas_response, schemas_status = self.get_schemas()
                if schemas_status != 200:
                    return jsonify(schemas_response), schemas_status

                schemas = schemas_response['schemas']

                # Check if table exists in any schema
                schema_name = None
                for schema in schemas:
                    if self.table_exists_in_schema(schema, table_name):
                        schema_name = schema
                        break

                if not schema_name:
                    raise ValueError(f"Table '{table_name}' does not exist in any schema.")

                table = Table(table_name, MetaData(), schema=schema_name, autoload_with=self.db.engine)
                insert_stmt = table.insert().values(table_data)
                conn = self.db.engine.connect()
                result = conn.execute(insert_stmt)
                conn.commit()
                conn.close()

                return jsonify({"message": "Record added successfully"}), 200
            except ValueError as ve:
                return jsonify({"error": str(ve)}), 400
            except Exception as e:
                return jsonify({"error": "Failed to add record", "details": str(e)}), 500

        @self.app.route('/update_record', methods=['PUT'])
        def update_record():
            try:
                # Extract data from request payload
                data = request.get_json()
                if not data or 'table_name' not in data or 'pk' not in data or 'update_data' not in data:
                    raise ValueError("Invalid JSON format. 'table', 'pk', and 'update_data' must be provided.")

                table_name = data['table_name']
                pk = data['pk']
                update_data = data['update_data']

                # Get list of schemas
                schemas_response, schemas_status = self.get_schemas()
                if schemas_status != 200:
                    return jsonify(schemas_response), schemas_status

                schemas = schemas_response['schemas']

                # Check if table exists in any schema
                schema_name = None
                for schema in schemas:
                    if self.table_exists_in_schema(schema, table_name):
                        schema_name = schema
                        break

                if not schema_name:
                    raise ValueError(f"Table '{table_name}' does not exist in any schema.")

                # Get the primary key column dynamically
                primary_key_col = self.get_primary_key_column(schema_name, table_name)
                if not primary_key_col:
                    raise ValueError(f"Primary key column not found for table '{table_name}'")

                # Build the update statement with schema name
                table = Table(table_name, MetaData(), schema=schema_name, autoload_with=self.db.engine)
                update_stmt = (
                    update(table)
                    .where(table.c[primary_key_col] == pk)
                    .values(update_data)
                )

                # Execute the update statement
                conn = self.db.engine.connect()
                result = conn.execute(update_stmt)
                conn.commit()
                conn.close()

                # Check if any rows were affected
                if result.rowcount == 0:
                    return jsonify({"message": f"No record found with primary key {pk} in table '{table_name}'"}), 404

                return jsonify({"message": "Record updated successfully"}), 200
            except ValueError as ve:
                return jsonify({"error": str(ve)}), 400
            except Exception as e:
                return jsonify({"error": "Failed to update record", "details": str(e)}), 500

        @self.app.route('/delete_record', methods=['DELETE'])
        def delete_record():
            try:
                # Extract data from request payload
                data = request.get_json()
                if not data or 'table_name' not in data or 'pk' not in data:
                    raise ValueError("Invalid JSON format. 'table' and 'pk' must be provided.")

                table_name = data['table_name']
                pk = data['pk']

                # Get list of schemas
                schemas_response, schemas_status = self.get_schemas()
                if schemas_status != 200:
                    return jsonify(schemas_response), schemas_status

                schemas = schemas_response['schemas']

                # Check if table exists in any schema
                schema_name = None
                for schema in schemas:
                    if self.table_exists_in_schema(schema, table_name):
                        schema_name = schema
                        break

                if not schema_name:
                    raise ValueError(f"Table '{table_name}' does not exist in any schema.")

                # Get the primary key column dynamically
                primary_key_col = self.get_primary_key_column(schema_name, table_name)
                if not primary_key_col:
                    raise ValueError(f"Primary key column not found for table '{table_name}'")

                # Build the delete statement with schema name
                table = Table(table_name, MetaData(), schema=schema_name, autoload_with=self.db.engine)
                delete_stmt = table.delete().where(table.c[primary_key_col] == pk)

                # Execute the delete statement
                conn = self.db.engine.connect()
                result = conn.execute(delete_stmt)
                conn.commit()
                conn.close()

                # Check if any rows were affected
                if result.rowcount == 0:
                    return jsonify({"message": f"No record found with primary key {pk} in table '{table_name}'"}), 404

                return jsonify({"message": "Record deleted successfully"}), 200
            except ValueError as ve:
                return jsonify({"error": str(ve)}), 400
            except Exception as e:
                return jsonify({"error": "Failed to delete record", "details": str(e)}), 500

    def get_schemas(self):
        try:
            inspector = inspect(self.db.engine)
            schemas = inspector.get_schema_names()
            return {"schemas": schemas}, 200
        except Exception as e:
            return {"error": str(e)}, 500

    def table_exists_in_schema(self, schema_name, table_name):
        inspector = inspect(self.db.engine)
        return table_name in inspector.get_table_names(schema=schema_name)

    def get_primary_key_column(self, schema_name, table_name):
        inspector = inspect(self.db.engine)
        try:
            primary_key_columns = inspector.get_pk_constraint(table_name, schema=schema_name)['constrained_columns']
            if primary_key_columns:
                return primary_key_columns[0]  # Assuming single-column primary key
        except Exception as e:
            print(f"Failed to get primary key for {table_name}: {str(e)}")
        return None

    def run(self):
        self.app.run()

if __name__ == '__main__':
    app_instance = FlaskApp()
    app_instance.run()
