from flask import Flask, render_template_string, request, redirect, url_for, session, send_file
from werkzeug.security import generate_password_hash, check_password_hash
import pandas as pd
import io
import os
from datetime import datetime

# Import sqlite3 first (always available in Python)
import sqlite3

# Check if PostgreSQL is available
try:
    import psycopg2
    from psycopg2 import sql
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')

# Database configuration
DATABASE_URL = os.environ.get('DATABASE_URL')

def get_db_connection():
    """Get database connection based on environment"""
    if DATABASE_URL and POSTGRES_AVAILABLE:
        # Use PostgreSQL in production
        conn = psycopg2.connect(DATABASE_URL)
        return conn, 'postgres'
    else:
        # Use SQLite for local development
        conn = sqlite3.connect('csv_cleaner.db')
        return conn, 'sqlite'

def init_db():
    """Initialize database tables"""
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    if db_type == 'postgres':
        # PostgreSQL syntax
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cleaning_history (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                filename VARCHAR(255),
                original_rows INTEGER,
                cleaned_rows INTEGER,
                operations TEXT,
                cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    else:
        # SQLite syntax
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cleaning_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                filename TEXT,
                original_rows INTEGER,
                cleaned_rows INTEGER,
                operations TEXT,
                cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ''')
    
    conn.commit()
    conn.close()

init_db()

# HTML Templates
LOGIN_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>CSV Cleaner - Login</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .container {
            background: white;
            padding: 40px;
            border-radius: 10px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            width: 400px;
        }
        h1 { color: #667eea; margin-bottom: 30px; text-align: center; }
        .form-group { margin-bottom: 20px; }
        label { display: block; margin-bottom: 5px; color: #333; font-weight: 500; }
        input[type="text"], input[type="password"] {
            width: 100%;
            padding: 12px;
            border: 2px solid #e0e0e0;
            border-radius: 5px;
            font-size: 14px;
            transition: border-color 0.3s;
        }
        input[type="text"]:focus, input[type="password"]:focus {
            outline: none;
            border-color: #667eea;
        }
        button {
            width: 100%;
            padding: 12px;
            background: #667eea;
            color: white;
            border: none;
            border-radius: 5px;
            font-size: 16px;
            cursor: pointer;
            transition: background 0.3s;
        }
        button:hover { background: #5568d3; }
        .message {
            padding: 10px;
            margin-bottom: 20px;
            border-radius: 5px;
            text-align: center;
        }
        .error { background: #fee; color: #c33; }
        .success { background: #efe; color: #3c3; }
        .toggle { text-align: center; margin-top: 15px; color: #666; }
        .toggle a { color: #667eea; text-decoration: none; }
        .toggle a:hover { text-decoration: underline; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🧹 CSV Cleaner</h1>
        {% if message %}
        <div class="message {{ message_type }}">{{ message }}</div>
        {% endif %}
        <form method="POST">
            <div class="form-group">
                <label>Username</label>
                <input type="text" name="username" required>
            </div>
            <div class="form-group">
                <label>Password</label>
                <input type="password" name="password" required>
            </div>
            <button type="submit">{{ action }}</button>
        </form>
        <div class="toggle">
            {% if action == 'Login' %}
            Don't have an account? <a href="{{ url_for('register') }}">Register here</a>
            {% else %}
            Already have an account? <a href="{{ url_for('login') }}">Login here</a>
            {% endif %}
        </div>
    </div>
</body>
</html>
'''

CLEANER_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>CSV Cleaner - Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #f5f5f5;
            min-height: 100vh;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .header-content {
            max-width: 1400px;
            margin: 0 auto;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .header h1 { font-size: 24px; }
        .user-info { display: flex; align-items: center; gap: 20px; }
        .logout-btn {
            padding: 8px 16px;
            background: rgba(255,255,255,0.2);
            color: white;
            border: 1px solid white;
            border-radius: 5px;
            cursor: pointer;
            text-decoration: none;
        }
        .logout-btn:hover { background: rgba(255,255,255,0.3); }
        .container {
            max-width: 1400px;
            margin: 30px auto;
            padding: 0 20px;
        }
        .card {
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }
        h2 { color: #333; margin-bottom: 20px; }
        h3 { color: #555; margin: 20px 0 15px 0; }
        .upload-area {
            border: 3px dashed #667eea;
            border-radius: 10px;
            padding: 40px;
            text-align: center;
            cursor: pointer;
            transition: background 0.3s;
        }
        .upload-area:hover { background: #f9f9ff; }
        .upload-area input[type="file"] { display: none; }
        .options { margin: 20px 0; }
        .checkbox-group {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
        }
        .checkbox-item {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .checkbox-item input[type="checkbox"] {
            width: 18px;
            height: 18px;
            cursor: pointer;
        }
        .checkbox-item label { cursor: pointer; }
        button[type="submit"] {
            padding: 12px 30px;
            background: #667eea;
            color: white;
            border: none;
            border-radius: 5px;
            font-size: 16px;
            cursor: pointer;
            transition: background 0.3s;
        }
        button[type="submit"]:hover { background: #5568d3; }
        button[type="submit"]:disabled {
            background: #ccc;
            cursor: not-allowed;
        }
        .result {
            margin-top: 20px;
            padding: 20px;
            background: #e8f5e9;
            border-left: 4px solid #4caf50;
            border-radius: 5px;
        }
        .result h3 { color: #2e7d32; margin-bottom: 10px; }
        .result ul { list-style: none; }
        .result li { padding: 5px 0; }
        .download-btn {
            display: inline-block;
            margin-top: 15px;
            padding: 10px 20px;
            background: #4caf50;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            margin-right: 10px;
        }
        .download-btn:hover { background: #45a049; }
        .history-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }
        .history-table th, .history-table td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        .history-table th {
            background: #667eea;
            color: white;
        }
        .history-table tr:hover { background: #f5f5f5; }
        .file-name {
            display: inline-block;
            margin-top: 10px;
            padding: 8px 16px;
            background: #e3f2fd;
            color: #1976d2;
            border-radius: 5px;
        }
        
        /* Data Preview Styles */
        .preview-section {
            margin-top: 30px;
        }
        .preview-tabs {
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
        }
        .tab-btn {
            padding: 10px 20px;
            background: #f0f0f0;
            border: none;
            border-radius: 5px 5px 0 0;
            cursor: pointer;
            font-size: 14px;
            transition: all 0.3s;
        }
        .tab-btn.active {
            background: #667eea;
            color: white;
        }
        .tab-btn:hover:not(.active) { background: #e0e0e0; }
        .preview-content {
            border: 1px solid #ddd;
            border-radius: 5px;
            padding: 20px;
            max-height: 500px;
            overflow: auto;
            background: #fafafa;
        }
        .data-table {
            width: 100%;
            border-collapse: collapse;
            background: white;
            font-size: 13px;
        }
        .data-table th {
            background: #667eea;
            color: white;
            padding: 10px;
            position: sticky;
            top: 0;
            text-align: left;
            font-weight: 600;
        }
        .data-table td {
            padding: 10px;
            border: 1px solid #e0e0e0;
        }
        .data-table tr:nth-child(even) { background: #f9f9f9; }
        .data-table tr:hover { background: #f0f0f0; }
        .tab-content {
            display: none;
        }
        .tab-content.active {
            display: block;
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }
        .stat-card {
            background: white;
            padding: 15px;
            border-radius: 8px;
            border-left: 4px solid #667eea;
        }
        .stat-label {
            font-size: 12px;
            color: #666;
            text-transform: uppercase;
        }
        .stat-value {
            font-size: 24px;
            font-weight: bold;
            color: #333;
            margin-top: 5px;
        }
        .no-data {
            text-align: center;
            padding: 40px;
            color: #999;
        }
        .db-badge {
            display: inline-block;
            padding: 4px 12px;
            background: #4caf50;
            color: white;
            border-radius: 12px;
            font-size: 12px;
            margin-left: 10px;
        }
    </style>
</head>
<body>
    <div class="header">
        <div class="header-content">
            <div>
                <h1>🧹 CSV Cleaner Dashboard <span class="db-badge">{{ db_type }}</span></h1>
            </div>
            <div class="user-info">
                <span>Welcome, {{ username }}!</span>
                <a href="{{ url_for('logout') }}" class="logout-btn">Logout</a>
            </div>
        </div>
    </div>
    
    <div class="container">
        <div class="card">
            <h2>Upload & Clean CSV</h2>
            <form method="POST" enctype="multipart/form-data" id="uploadForm">
                <div class="upload-area" onclick="document.getElementById('fileInput').click()">
                    <p>📁 Click to upload CSV file or drag and drop</p>
                    <input type="file" id="fileInput" name="file" accept=".csv" required onchange="displayFileName()">
                    <div id="fileName" class="file-name" style="display:none;"></div>
                </div>
                
                <div class="options">
                    <h3>Cleaning Options:</h3>
                    <div class="checkbox-group">
                        <div class="checkbox-item">
                            <input type="checkbox" id="remove_duplicates" name="remove_duplicates" checked>
                            <label for="remove_duplicates">Remove Duplicate Rows</label>
                        </div>
                        <div class="checkbox-item">
                            <input type="checkbox" id="remove_null" name="remove_null" checked>
                            <label for="remove_null">Remove Rows with NULL/Empty Values</label>
                        </div>
                        <div class="checkbox-item">
                            <input type="checkbox" id="trim_whitespace" name="trim_whitespace" checked>
                            <label for="trim_whitespace">Trim Whitespace</label>
                        </div>
                        <div class="checkbox-item">
                            <input type="checkbox" id="remove_empty_cols" name="remove_empty_cols">
                            <label for="remove_empty_cols">Remove Empty Columns</label>
                        </div>
                        <div class="checkbox-item">
                            <input type="checkbox" id="standardize_case" name="standardize_case">
                            <label for="standardize_case">Standardize Text Case (lowercase)</label>
                        </div>
                        <div class="checkbox-item">
                            <input type="checkbox" id="remove_special_chars" name="remove_special_chars">
                            <label for="remove_special_chars">Remove Special Characters</label>
                        </div>
                    </div>
                </div>
                
                <button type="submit">Clean CSV</button>
            </form>
            
            {% if result %}
            <div class="result">
                <h3>✅ Cleaning Complete!</h3>
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-label">Original Rows</div>
                        <div class="stat-value">{{ result.original_rows }}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Cleaned Rows</div>
                        <div class="stat-value">{{ result.cleaned_rows }}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Rows Removed</div>
                        <div class="stat-value">{{ result.rows_removed }}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Columns</div>
                        <div class="stat-value">{{ result.columns }}</div>
                    </div>
                </div>
                <p><strong>Operations Applied:</strong> {{ result.operations }}</p>
                <a href="{{ url_for('download') }}" class="download-btn">📥 Download Cleaned CSV</a>
            </div>
            
            <!-- Data Preview Section -->
            <div class="preview-section">
                <h3>📊 Data Preview</h3>
                <div class="preview-tabs">
                    <button class="tab-btn active" onclick="switchTab('original')">Original Data</button>
                    <button class="tab-btn" onclick="switchTab('cleaned')">Cleaned Data</button>
                </div>
                
                <div id="original-tab" class="tab-content active">
                    <h4 style="margin-bottom: 15px;">Original Data (First 20 rows)</h4>
                    {% if original_preview %}
                    <div class="preview-content">
                        {{ original_preview|safe }}
                    </div>
                    {% else %}
                    <div class="no-data">No data to display</div>
                    {% endif %}
                </div>
                
                <div id="cleaned-tab" class="tab-content">
                    <h4 style="margin-bottom: 15px;">Cleaned Data (First 20 rows)</h4>
                    {% if cleaned_preview %}
                    <div class="preview-content">
                        {{ cleaned_preview|safe }}
                    </div>
                    {% else %}
                    <div class="no-data">No data to display</div>
                    {% endif %}
                </div>
            </div>
            {% endif %}
        </div>
        
        <div class="card">
            <h2>📜 Cleaning History</h2>
            {% if history %}
            <table class="history-table">
                <thead>
                    <tr>
                        <th>Filename</th>
                        <th>Original Rows</th>
                        <th>Cleaned Rows</th>
                        <th>Operations</th>
                        <th>Date</th>
                    </tr>
                </thead>
                <tbody>
                    {% for record in history %}
                    <tr>
                        <td>{{ record[0] }}</td>
                        <td>{{ record[1] }}</td>
                        <td>{{ record[2] }}</td>
                        <td>{{ record[3] }}</td>
                        <td>{{ record[4] }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% else %}
            <p>No cleaning history yet. Upload a CSV to get started!</p>
            {% endif %}
        </div>
    </div>
    
    <script>
        function displayFileName() {
            const input = document.getElementById('fileInput');
            const display = document.getElementById('fileName');
            if (input.files.length > 0) {
                display.textContent = '📄 ' + input.files[0].name;
                display.style.display = 'inline-block';
            }
        }
        
        function switchTab(tabName) {
            document.querySelectorAll('.tab-content').forEach(tab => {
                tab.classList.remove('active');
            });
            document.querySelectorAll('.tab-btn').forEach(btn => {
                btn.classList.remove('active');
            });
            
            document.getElementById(tabName + '-tab').classList.add('active');
            event.target.classList.add('active');
        }
    </script>
</body>
</html>
'''

# Routes
@app.route('/')
def index():
    if 'user_id' in session:
        return redirect(url_for('cleaner'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        conn, db_type = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id, password FROM users WHERE username = %s' if db_type == 'postgres' else 'SELECT id, password FROM users WHERE username = ?', (username,))
        user = cursor.fetchone()
        conn.close()
        
        if user and check_password_hash(user[1], password):
            session['user_id'] = user[0]
            session['username'] = username
            return redirect(url_for('cleaner'))
        else:
            return render_template_string(LOGIN_TEMPLATE, action='Login', 
                                        message='Invalid credentials', message_type='error')
    
    return render_template_string(LOGIN_TEMPLATE, action='Login')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        hashed = generate_password_hash(password)
        
        try:
            conn, db_type = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO users (username, password) VALUES (%s, %s)' if db_type == 'postgres' else 'INSERT INTO users (username, password) VALUES (?, ?)', (username, hashed))
            conn.commit()
            conn.close()
            return render_template_string(LOGIN_TEMPLATE, action='Login', 
                                        message='Registration successful! Please login.', 
                                        message_type='success')
        except Exception as e:
            return render_template_string(LOGIN_TEMPLATE, action='Register', 
                                        message='Username already exists', message_type='error')
    
    return render_template_string(LOGIN_TEMPLATE, action='Register')

def df_to_html_table(df, max_rows=20):
    """Convert DataFrame to HTML table"""
    df_preview = df.head(max_rows)
    
    html = '<table class="data-table"><thead><tr>'
    for col in df_preview.columns:
        html += f'<th>{col}</th>'
    html += '</tr></thead><tbody>'
    
    for _, row in df_preview.iterrows():
        html += '<tr>'
        for val in row:
            html += f'<td>{val}</td>'
        html += '</tr>'
    html += '</tbody></table>'
    
    if len(df) > max_rows:
        html += f'<p style="margin-top: 10px; color: #666; font-size: 13px;">Showing {max_rows} of {len(df)} rows</p>'
    
    return html

@app.route('/cleaner', methods=['GET', 'POST'])
def cleaner():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    result = None
    original_preview = None
    cleaned_preview = None
    
    # Get database type for display
    _, db_type = get_db_connection()
    db_display = 'PostgreSQL' if db_type == 'postgres' else 'SQLite'
    
    if request.method == 'POST':
        file = request.files['file']
        if file and file.filename.endswith('.csv'):
            # Read CSV
            df_original = pd.read_csv(file)
            df = df_original.copy()
            original_rows = len(df)
            operations = []
            
            # Get cleaning options
            if 'remove_duplicates' in request.form:
                df = df.drop_duplicates()
                operations.append('Remove Duplicates')
            
            if 'remove_null' in request.form:
                df = df.dropna()
                operations.append('Remove NULL Values')
            
            if 'trim_whitespace' in request.form:
                df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                operations.append('Trim Whitespace')
            
            if 'remove_empty_cols' in request.form:
                df = df.dropna(axis=1, how='all')
                operations.append('Remove Empty Columns')
            
            if 'standardize_case' in request.form:
                df = df.applymap(lambda x: x.lower() if isinstance(x, str) else x)
                operations.append('Standardize Case')
            
            if 'remove_special_chars' in request.form:
                df = df.applymap(lambda x: ''.join(e for e in x if e.isalnum() or e.isspace()) 
                               if isinstance(x, str) else x)
                operations.append('Remove Special Characters')
            
            cleaned_rows = len(df)
            
            # Generate HTML previews
            original_preview = df_to_html_table(df_original)
            cleaned_preview = df_to_html_table(df)
            
            # Store cleaned data in session
            session['cleaned_data'] = df.to_csv(index=False)
            session['filename'] = file.filename
            
            # Save to database
            conn, db_type = get_db_connection()
            cursor = conn.cursor()
            
            if db_type == 'postgres':
                cursor.execute('''INSERT INTO cleaning_history 
                            (user_id, filename, original_rows, cleaned_rows, operations)
                            VALUES (%s, %s, %s, %s, %s)''',
                         (session['user_id'], file.filename, original_rows, cleaned_rows, 
                          ', '.join(operations)))
            else:
                cursor.execute('''INSERT INTO cleaning_history 
                            (user_id, filename, original_rows, cleaned_rows, operations)
                            VALUES (?, ?, ?, ?, ?)''',
                         (session['user_id'], file.filename, original_rows, cleaned_rows, 
                          ', '.join(operations)))
            
            conn.commit()
            conn.close()
            
            result = {
                'original_rows': original_rows,
                'cleaned_rows': cleaned_rows,
                'rows_removed': original_rows - cleaned_rows,
                'columns': len(df.columns),
                'operations': ', '.join(operations) if operations else 'None'
            }
    
    # Get user's history
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    
    if db_type == 'postgres':
        cursor.execute('''SELECT filename, original_rows, cleaned_rows, operations, 
                     TO_CHAR(cleaned_at, 'YYYY-MM-DD HH24:MI:SS')
                     FROM cleaning_history 
                     WHERE user_id = %s 
                     ORDER BY cleaned_at DESC LIMIT 10''', (session['user_id'],))
    else:
        cursor.execute('''SELECT filename, original_rows, cleaned_rows, operations, 
                     datetime(cleaned_at, 'localtime') 
                     FROM cleaning_history 
                     WHERE user_id = ? 
                     ORDER BY cleaned_at DESC LIMIT 10''', (session['user_id'],))
    
    history = cursor.fetchall()
    conn.close()
    
    return render_template_string(CLEANER_TEMPLATE, 
                                 username=session['username'], 
                                 result=result, 
                                 history=history,
                                 original_preview=original_preview,
                                 cleaned_preview=cleaned_preview,
                                 db_type=db_display)

@app.route('/download')
def download():
    if 'user_id' not in session or 'cleaned_data' not in session:
        return redirect(url_for('login'))
    
    output = io.BytesIO()
    output.write(session['cleaned_data'].encode('utf-8'))
    output.seek(0)
    
    filename = 'cleaned_' + session.get('filename', 'data.csv')
    
    return send_file(output, mimetype='text/csv', as_attachment=True, 
                    download_name=filename)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)