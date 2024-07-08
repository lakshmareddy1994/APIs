from flask import Flask, request, jsonify, render_template
from flask_sqlalchemy import SQLAlchemy
import os
from dotenv import load_dotenv
from sqlalchemy import update, MetaData, Table, inspect
import urllib.parse

# Initialize SQLAlchemy instance
db = SQLAlchemy()

class FlaskApp:
    def __init__(self):
        self.app = Flask(__name__)
        self._load_config()
        self._setup_database()
        self._setup_routes()

    def _load_config(self):
        load_dotenv()  # Load environment variables from .env file
        self.app.config['SQLALCHEMY_DATABASE_URI'] = f"mssql+pyodbc://{os.getenv('DB_USER')}:{urllib.parse.quote_plus(os.getenv('DB_PASSWORD'))}@{os.getenv('DB_SERVER')}/{os.getenv('DB_DATABASE')}?driver=ODBC+Driver+17+for+SQL+Server"
        self.app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    def _setup_database(self):
        global db
        db.init_app(self.app)
        with self.app.app_context():
            self.metadata = MetaData()
            self.metadata.reflect(bind=db.engine)

    def fetch_schema_name(self, table_name):
        try:
            inspector = inspect(db.engine)
            for schema in inspector.get_schema_names():
                for table in inspector.get_table_names(schema=schema):
                    if table == table_name:
                        return schema
            return None
        except Exception as e:
            print(f"Error fetching schema name: {e}")
            return None

    def read_data_from_mssql(self, schema_name, table_name):
        try:
            table = Table(table_name, self.metadata, schema=schema_name, autoload_with=db.engine)
            result = db.session.execute(table.select()).fetchall()
            return [dict(row._mapping) for row in result]
        except Exception as e:
            return {"error": str(e)}

    def add_record_to_table(self, schema_name, table_name, table_data):
        try:
            table = Table(table_name, self.metadata, schema=schema_name, autoload_with=db.engine)
            insert_query = table.insert().values(**table_data)
            db.session.execute(insert_query)
            db.session.commit()
            return {"message": "Record added successfully"}
        except Exception as e:
            return {"error": str(e)}

    def update_record_in_table(self, schema_name, table_name, pk, update_data):
        try:
            table = Table(table_name, self.metadata, schema=schema_name, autoload_with=db.engine)
            pk_column = [col for col in table.primary_key][0]
            update_query = update(table).where(pk_column == pk).values(**update_data)
            db.session.execute(update_query)
            db.session.commit()
            return {"message": "Record updated successfully"}
        except Exception as e:
            return {"error": str(e)}

    def delete_record_from_table(self, schema_name, table_name, pk):
        try:
            table = Table(table_name, self.metadata, schema=schema_name, autoload_with=db.engine)
            pk_column = [col for col in table.primary_key][0]
            delete_query = table.delete().where(pk_column == pk)
            db.session.execute(delete_query)
            db.session.commit()
            return {"message": "Record deleted successfully"}
        except Exception as e:
            return {"error": str(e)}

    def _setup_routes(self):
        @self.app.route('/')
        def index():
            return render_template('index.html')

        @self.app.route('/read_table', methods=['POST'])
        def read_table():
            request_data = request.get_json()
            table_name = request_data.get('table_name')

            if not table_name:
                return jsonify({"error": "Table name is not provided."}), 400

            schema_name = self.fetch_schema_name(table_name)

            if not schema_name:
                return jsonify({"error": f"Schema for table '{table_name}' not found."}), 404

            final_status = self.read_data_from_mssql(schema_name, table_name)

            if "error" in final_status:
                return jsonify(final_status), 500

            return jsonify(final_status)

        @self.app.route('/add_record', methods=['POST'])
        def add_record():
            request_data = request.get_json()
            table_name = request_data.get('table_name')
            table_data = request_data.get('table_data')

            if not table_name or not table_data:
                return jsonify({"error": "Table name or data is not provided."}), 400

            schema_name = self.fetch_schema_name(table_name)

            if not schema_name:
                return jsonify({"error": f"Schema for table '{table_name}' not found."}), 404

            final_status = self.add_record_to_table(schema_name, table_name, table_data)

            if "error" in final_status:
                return jsonify(final_status), 500

            return jsonify(final_status)

        @self.app.route('/update_record', methods=['PUT'])
        def update_record():
            request_data = request.get_json()
            table_name = request_data.get('table_name')
            pk = request_data.get('pk')
            update_data = request_data.get('update_data')

            if not table_name or not pk or not update_data:
                return jsonify({"error": "Table name, primary key (pk), or update data is not provided."}), 400

            schema_name = self.fetch_schema_name(table_name)

            if not schema_name:
                return jsonify({"error": f"Schema for table '{table_name}' not found."}), 404

            final_status = self.update_record_in_table(schema_name, table_name, pk, update_data)

            if "error" in final_status:
                return jsonify(final_status), 500

            return jsonify(final_status)

        @self.app.route('/delete_record', methods=['DELETE'])
        def delete_record():
            request_data = request.get_json()
            table_name = request_data.get('table_name')
            pk = request_data.get('pk')

            if not table_name or not pk:
                return jsonify({"error": "Table name or primary key (pk) is not provided."}), 400

            schema_name = self.fetch_schema_name(table_name)

            if not schema_name:
                return jsonify({"error": f"Schema for table '{table_name}' not found."}), 404

            final_status = self.delete_record_from_table(schema_name, table_name, pk)

            if "error" in final_status:
                return jsonify(final_status), 500

            return jsonify(final_status)

    def run(self):
        self.app.run()

if __name__ == '__main__':
    flask_app = FlaskApp()
    flask_app.run()
