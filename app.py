import io
import os
import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
import time
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, time as dt_time
import pytz

# Configure timezone for Bolivia
BOLIVIA_TZ = pytz.timezone('America/La_Paz')

def get_bolivia_now():
    """Get current datetime in Bolivia timezone"""
    return datetime.now(BOLIVIA_TZ)

def get_bolivia_today():
    """Get today's date in Bolivia timezone"""
    return get_bolivia_now().date()

# Configure page
st.set_page_config(
    page_title="Control de Proveedores",
    page_icon="🚚",
    layout="wide"
)

# Custom CSS for enhanced tab visibility - UNCHANGED
st.markdown("""
<style>
/* Tab styling */
.stTabs [data-baseweb="tab-list"] {
    gap: 20px;
    background-color: #f0f2f6;
    padding: 10px;
    border-radius: 10px;
    margin-bottom: 20px;
}

.stTabs [data-baseweb="tab"] {
    height: 60px;
    background-color: white;
    border-radius: 8px;
    padding: 0 20px;
    border: 2px solid #e1e5e9;
    font-weight: bold;
    font-size: 16px;
}

.stTabs [aria-selected="true"] {
    background-color: #1f77b4 !important;
    color: white !important;
    border-color: #1f77b4 !important;
    box-shadow: 0 4px 8px rgba(31, 119, 180, 0.3);
}

/* Arrival tab content styling */
.arrival-container {
    background: linear-gradient(135deg, #e3f2fd 0%, #f3e5f5 100%);
    border: 3px solid #2196f3;
    border-radius: 15px;
    padding: 25px;
    margin: 15px 0;
    box-shadow: 0 6px 20px rgba(33, 150, 243, 0.15);
}

.arrival-header {
    background-color: #2196f3;
    color: white;
    padding: 15px;
    border-radius: 10px;
    margin-bottom: 20px;
    text-align: center;
    font-weight: bold;
    font-size: 18px;
}

/* Service tab content styling */
.service-container {
    background: linear-gradient(135deg, #e8f5e8 0%, #fff3e0 100%);
    border: 3px solid #4caf50;
    border-radius: 15px;
    padding: 25px;
    margin: 15px 0;
    box-shadow: 0 6px 20px rgba(76, 175, 80, 0.15);
}

.service-header {
    background-color: #4caf50;
    color: white;
    padding: 15px;
    border-radius: 10px;
    margin-bottom: 20px;
    text-align: center;
    font-weight: bold;
    font-size: 18px;
}

/* Button styling */
.arrival-container .stButton > button {
    background-color: #2196f3;
    color: white;
    border: none;
    border-radius: 8px;
    font-weight: bold;
    padding: 10px 20px;
    box-shadow: 0 3px 6px rgba(33, 150, 243, 0.3);
}

.service-container .stButton > button {
    background-color: #4caf50;
    color: white;
    border: none;
    border-radius: 8px;
    font-weight: bold;
    padding: 10px 20px;
    box-shadow: 0 3px 6px rgba(76, 175, 80, 0.3);
}

/* Info boxes */
.arrival-info {
    background-color: rgba(33, 150, 243, 0.1);
    border-left: 5px solid #2196f3;
    padding: 15px;
    border-radius: 0 8px 8px 0;
    margin: 10px 0;
}

.service-info {
    background-color: rgba(76, 175, 80, 0.1);
    border-left: 5px solid #4caf50;
    padding: 15px;
    border-radius: 0 8px 8px 0;
    margin: 10px 0;
}

/* Visual separator */
.tab-separator {
    height: 4px;
    background: linear-gradient(90deg, #2196f3 0%, #4caf50 100%);
    margin: 20px 0;
    border-radius: 2px;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# 1. Google Sheets Configuration - MIGRATED FROM SHAREPOINT
# ─────────────────────────────────────────────────────────────
@st.cache_resource
def setup_google_sheets():
    """Configurar conexión a Google Sheets"""
    try:
        credentials_info = dict(st.secrets["google_service_account"])
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        gc = gspread.authorize(credentials)
        return gc
    except Exception as e:
        st.error(f"❌ Error conectando: {str(e)}")
        return None

# ─────────────────────────────────────────────────────────────
# 2. Google Sheets Download Functions - MIGRATED FROM SHAREPOINT EXCEL
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)  # Reduced TTL for real-time management
def download_sheets_to_memory():
    """Download all sheets from Google Sheets - REPLACES SharePoint Excel download"""
    try:
        gc = setup_google_sheets()
        if not gc:
            return None, None, None
        
        spreadsheet = gc.open(st.secrets["GOOGLE_SHEET_NAME"])
        
        # Load credentials sheet
        try:
            credentials_ws = spreadsheet.worksheet("proveedor_credencial")
            credentials_data = credentials_ws.get_all_records()
            if credentials_data:
                credentials_df = pd.DataFrame(credentials_data)
                # Ensure all columns are strings for consistency
                for col in credentials_df.columns:
                    credentials_df[col] = credentials_df[col].astype(str)
            else:
                # Fallback to raw values
                all_values = credentials_ws.get_all_values()
                if all_values and len(all_values) > 1:
                    credentials_df = pd.DataFrame(all_values[1:], columns=all_values[0])
                else:
                    credentials_df = pd.DataFrame(columns=['usuario', 'password', 'Email', 'cc'])
        except gspread.WorksheetNotFound:
            credentials_df = pd.DataFrame(columns=['usuario', 'password', 'Email', 'cc'])
        
        # Load reservas sheet
        try:
            reservas_ws = spreadsheet.worksheet("proveedor_reservas")
            reservas_data = reservas_ws.get_all_records()
            if reservas_data:
                reservas_df = pd.DataFrame(reservas_data)
                # Ensure Orden_de_compra is string
                if 'Orden_de_compra' in reservas_df.columns:
                    reservas_df['Orden_de_compra'] = reservas_df['Orden_de_compra'].astype(str)
            else:
                # Fallback to raw values
                all_values = reservas_ws.get_all_values()
                if all_values and len(all_values) > 1:
                    reservas_df = pd.DataFrame(all_values[1:], columns=all_values[0])
                    # Ensure Orden_de_compra is string
                    if 'Orden_de_compra' in reservas_df.columns:
                        reservas_df['Orden_de_compra'] = reservas_df['Orden_de_compra'].astype(str)
                else:
                    reservas_df = pd.DataFrame(columns=['Fecha', 'Hora', 'Proveedor', 'Numero_de_bultos', 'Orden_de_compra'])
        except gspread.WorksheetNotFound:
            reservas_df = pd.DataFrame(columns=['Fecha', 'Hora', 'Proveedor', 'Numero_de_bultos', 'Orden_de_compra'])
            


        # Load or create gestion sheet
        try:
            gestion_ws = spreadsheet.worksheet("proveedor_gestion")
            gestion_data = gestion_ws.get_all_records()
            if gestion_data:
                gestion_df = pd.DataFrame(gestion_data)
            else:
                # Fallback to raw values
                all_values = gestion_ws.get_all_values()
                if all_values and len(all_values) > 1:
                    gestion_df = pd.DataFrame(all_values[1:], columns=all_values[0])
                else:
                    gestion_df = pd.DataFrame(columns=[
                        'Orden_de_compra', 'Proveedor', 'Numero_de_bultos',
                        'Hora_llegada', 'Hora_inicio_atencion', 'Hora_fin_atencion',
                        'Tiempo_espera', 'Tiempo_atencion', 'Tiempo_total', 'Tiempo_retraso',
                        'numero_de_semana', 'hora_de_reserva'
                    ])
        except gspread.WorksheetNotFound:
            # Create gestion sheet if it doesn't exist
            try:
                gestion_ws = spreadsheet.add_worksheet("proveedor_gestion", rows=200, cols=12)
                # Add headers
                headers = [
                    'Orden_de_compra', 'Proveedor', 'Numero_de_bultos',
                    'Hora_llegada', 'Hora_inicio_atencion', 'Hora_fin_atencion',
                    'Tiempo_espera', 'Tiempo_atencion', 'Tiempo_total', 'Tiempo_retraso',
                    'numero_de_semana', 'hora_de_reserva'
                ]
                gestion_ws.update('A1:L1', [headers])
                gestion_df = pd.DataFrame(columns=headers)
            except Exception as e:
                st.warning(f"No se pudo crear hoja de gestión: {e}")
                gestion_df = pd.DataFrame(columns=[
                    'Orden_de_compra', 'Proveedor', 'Numero_de_bultos',
                    'Hora_llegada', 'Hora_inicio_atencion', 'Hora_fin_atencion',
                    'Tiempo_espera', 'Tiempo_atencion', 'Tiempo_total', 'Tiempo_retraso',
                    'numero_de_semana', 'hora_de_reserva'
                ])
        
        return credentials_df, reservas_df, gestion_df
        
    except Exception as e:
        st.error(f"Error descargando datos: {str(e)}")
        return None, None, None

def save_gestion_to_sheets(new_record):
    """Save new management record to Google Sheets - REPLACES SharePoint Excel save"""
    try:
        # Load current data
        credentials_df, reservas_df, gestion_df = download_sheets_to_memory()
        
        if reservas_df is None:
            return False
        
        # Get Google Sheets connection
        gc = setup_google_sheets()
        if not gc:
            return False
        
        spreadsheet = gc.open(st.secrets["GOOGLE_SHEET_NAME"])
        gestion_ws = spreadsheet.worksheet("proveedor_gestion")
        
        # Prepare new row data - MAINTAIN EXACT FORMAT
        new_row_data = [
            new_record.get('Orden_de_compra', ''),           # A: Orden_de_compra
            new_record.get('Proveedor', ''),                 # B: Proveedor
            str(new_record.get('Numero_de_bultos', '')),     # C: Numero_de_bultos
            new_record.get('Hora_llegada', ''),              # D: Hora_llegada
            new_record.get('Hora_inicio_atencion', ''),      # E: Hora_inicio_atencion
            new_record.get('Hora_fin_atencion', ''),         # F: Hora_fin_atencion
            str(new_record.get('Tiempo_espera', '')),        # G: Tiempo_espera
            str(new_record.get('Tiempo_atencion', '')),      # H: Tiempo_atencion
            str(new_record.get('Tiempo_total', '')),         # I: Tiempo_total
            str(new_record.get('Tiempo_retraso', '')),       # J: Tiempo_retraso
            str(new_record.get('numero_de_semana', '')),     # K: numero_de_semana
            str(new_record.get('hora_de_reserva', ''))       # L: hora_de_reserva
        ]
        
        # Append the new record
        gestion_ws.append_row(new_row_data, value_input_option='RAW')
        
        # Clear cache after successful save
        download_sheets_to_memory.clear()
        
        return True
        
    except Exception as e:
        st.error(f"❌ Error guardando registro en Google Sheets: {str(e)}")
        return False

def update_sheets_record(orden_compra, update_data):
    """Update existing record in Google Sheets - REPLACES SharePoint Excel update"""
    try:
        gc = setup_google_sheets()
        if not gc:
            return False
        
        spreadsheet = gc.open(st.secrets["GOOGLE_SHEET_NAME"])
        gestion_ws = spreadsheet.worksheet("proveedor_gestion")
        
        # Get all data
        all_values = gestion_ws.get_all_values()
        
        # Find the row to update
        target_row_index = None
        for i, row in enumerate(all_values[1:], start=1):  # Skip header, start counting from 1
            if len(row) > 0 and str(row[0]).strip() == str(orden_compra).strip():
                target_row_index = i
                break
        
        if target_row_index is None:
            st.error("No se encontró el registro para actualizar")
            return False
        
        # Get the current row data
        current_row = all_values[target_row_index].copy()
        
        # Ensure row has enough columns (12 columns total)
        while len(current_row) < 12:
            current_row.append('')
        
        # Column mapping (0-based index)
        col_mapping = {
            'Hora_llegada': 3,           # D = index 3
            'Hora_inicio_atencion': 4,   # E = index 4
            'Hora_fin_atencion': 5,      # F = index 5
            'Tiempo_espera': 6,          # G = index 6
            'Tiempo_atencion': 7,        # H = index 7
            'Tiempo_total': 8,           # I = index 8
            'Tiempo_retraso': 9,         # J = index 9
            'numero_de_semana': 10,      # K = index 10
            'hora_de_reserva': 11        # L = index 11
        }
        
        # Update the row data
        for field, value in update_data.items():
            if field in col_mapping:
                col_index = col_mapping[field]
                
                # Handle None values and ensure proper string conversion
                if value is None or str(value).lower() in ['none', 'nan', '']:
                    current_row[col_index] = ''
                else:
                    current_row[col_index] = str(value)
        
        # Update the entire row (row numbers are 1-based for gspread)
        row_number = target_row_index + 1  # Convert to 1-based
        range_name = f"A{row_number}:L{row_number}"
        
        # Update the row
        gestion_ws.update(range_name, [current_row], value_input_option='RAW')
        
        # Clear cache after successful update
        download_sheets_to_memory.clear()
        
        return True
        
    except Exception as e:
        st.error(f"❌ Error actualizando registro: {str(e)}")
        return False


# ─────────────────────────────────────────────────────────────
# 3. Helper Functions - UNCHANGED TIME PARSING AND CALCULATIONS
# ─────────────────────────────────────────────────────────────
def get_today_reservations(reservas_df):
    """Get today's reservations"""
    today = get_bolivia_today().strftime('%Y-%m-%d')
    return reservas_df[reservas_df['Fecha'].astype(str).str.contains(today, na=False)]

def parse_time_range(time_range_str):
    """Parse time range string (e.g., '09:00-09:30' or '09:00 - 09:30') and return start time"""
    try:
        # Handle both formats: "12:00-12:30" and "12:00 - 12:30"
        if '-' in time_range_str:
            start_time_str = time_range_str.split('-')[0].strip()
            return datetime.strptime(start_time_str, '%H:%M').time()
        return None
    except:
        return None

def parse_single_time(time_str):
    """Parse single time string (e.g., '09:00') and return time object"""
    try:
        return datetime.strptime(time_str.strip(), '%H:%M').time()
    except:
        return None
        
def parse_combined_time_slots(time_str):
    """Parse comma-separated time slots and return the first (start) time"""
    try:
        if ',' in time_str:
            # Take the first time slot for combined bookings
            first_slot = time_str.split(',')[0].strip()
            # Remove seconds if present (e.g., "09:00:00" -> "09:00")
            if first_slot.count(':') == 2:
                first_slot = ':'.join(first_slot.split(':')[:2])
            return datetime.strptime(first_slot, '%H:%M').time()
        return None
    except:
        return None        

def calculate_time_difference(start_datetime, end_datetime):
    """Calculate time difference in minutes"""
    if start_datetime and end_datetime:
        # Ensure both are datetime objects
        if isinstance(start_datetime, str):
            start_datetime = datetime.fromisoformat(start_datetime)
        if isinstance(end_datetime, str):
            end_datetime = datetime.fromisoformat(end_datetime)
            
        diff = end_datetime - start_datetime
        return int(diff.total_seconds() / 60)
    return None

def combine_date_time(date_part, time_part):
    """Combine date and time into datetime"""
    return datetime.combine(date_part, time_part)

def format_datetime_no_zero_padding(dt):
    """Format datetime with single digit hours (9:00:00 not 09:00:00)"""
    if dt is None:
        return None
    
    # Get components
    year = dt.year
    month = dt.month
    day = dt.day
    hour = dt.hour  # This will be single digit for 1-9
    minute = dt.minute
    second = dt.second
    
    # Format with single digit hour when needed
    return f"{year}-{month:02d}-{day:02d} {hour}:{minute:02d}:{second:02d}"

def parse_datetime_flexible(datetime_str):
    """Parse datetime string that may have single or double digit hours"""
    if not datetime_str or str(datetime_str).lower() in ['none', 'nan', '']:
        return None
    
    datetime_str = str(datetime_str).strip()
    
    try:
        # First try standard ISO format (with zero-padded hours)
        return datetime.fromisoformat(datetime_str)
    except ValueError:
        try:
            # Try parsing with single digit hours manually
            # Format: "2025-07-08 9:00:00"
            if ' ' in datetime_str and ':' in datetime_str:
                date_part, time_part = datetime_str.split(' ', 1)
                
                # Parse date part
                year, month, day = map(int, date_part.split('-'))
                
                # Parse time part
                time_components = time_part.split(':')
                hour = int(time_components[0])
                minute = int(time_components[1]) if len(time_components) > 1 else 0
                second = int(time_components[2]) if len(time_components) > 2 else 0
                
                return datetime(year, month, day, hour, minute, second)
            else:
                return None
        except (ValueError, IndexError):
            return None

# ─────────────────────────────────────────────────────────────
# 4. Dashboard Helper Functions - UNCHANGED
# ─────────────────────────────────────────────────────────────
def get_current_week():
    """Get current week number"""
    return get_bolivia_now().isocalendar()[1]

def get_completed_weeks_data(gestion_df, weeks_back):
    """Get data for completed weeks only"""
    if gestion_df.empty:
        return pd.DataFrame()
    
    current_week = get_current_week()
    # Get weeks that are fully completed (exclude current week)
    target_weeks = [current_week - i for i in range(1, weeks_back + 1)]
    
    # Filter data for target weeks
    filtered_df = gestion_df[
        (gestion_df['numero_de_semana'].astype(str).str.isdigit()) &
        (pd.to_numeric(gestion_df['numero_de_semana'], errors='coerce').isin(target_weeks)) &
        (gestion_df['Tiempo_total'].notna())  # Only completed records
    ].copy()
    
    return filtered_df

def aggregate_by_week(df, provider_filter=None):
    """Aggregate data by week"""
    if df.empty:
        return pd.DataFrame()
    
    # Filter by provider if specified
    if provider_filter and provider_filter != "Todos":
        df = df[df['Proveedor'] == provider_filter]
    
    # Convert numeric columns
    for col in ['Tiempo_espera', 'Tiempo_atencion', 'Tiempo_total', 'Tiempo_retraso']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Aggregate by week
    weekly_data = df.groupby('numero_de_semana').agg({
        'Tiempo_espera': 'mean',
        'Tiempo_atencion': 'mean', 
        'Tiempo_total': 'mean',
        'Tiempo_retraso': 'mean'
    }).round(1).reset_index()
    
    return weekly_data

def aggregate_by_hour_from_filtered(filtered_df, provider_filter=None):
    """Aggregate data by reservation hour from already filtered data"""
    if filtered_df.empty:
        return pd.DataFrame()
    
    # Filter by provider if specified
    if provider_filter and provider_filter != "Todos":
        filtered_df = filtered_df[filtered_df['Proveedor'] == provider_filter]
    
    if filtered_df.empty:
        return pd.DataFrame()
    
    # Convert numeric columns
    for col in ['Tiempo_espera', 'Tiempo_atencion', 'Tiempo_total', 'Tiempo_retraso']:
        filtered_df[col] = pd.to_numeric(filtered_df[col], errors='coerce')
    
    # Filter out records without reservation hour
    filtered_df = filtered_df[filtered_df['hora_de_reserva'].notna()]
    
    if filtered_df.empty:
        return pd.DataFrame()
    
    # Aggregate by hour
    hourly_data = filtered_df.groupby('hora_de_reserva').agg({
        'Tiempo_espera': 'mean',
        'Tiempo_atencion': 'mean',
        'Tiempo_total': 'mean', 
        'Tiempo_retraso': 'mean'
    }).round(1).reset_index()
    
    return hourly_data

def create_weekly_times_chart(weekly_data):
    """Create chart for weekly time metrics"""
    if weekly_data.empty:
        return None
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=weekly_data['numero_de_semana'],
        y=weekly_data['Tiempo_espera'],
        mode='lines+markers',
        name='Tiempo de Espera',
        line=dict(color='#FF6B6B')
    ))
    
    fig.add_trace(go.Scatter(
        x=weekly_data['numero_de_semana'],
        y=weekly_data['Tiempo_atencion'],
        mode='lines+markers', 
        name='Tiempo de Atención',
        line=dict(color='#4ECDC4')
    ))
    
    fig.add_trace(go.Scatter(
        x=weekly_data['numero_de_semana'],
        y=weekly_data['Tiempo_total'],
        mode='lines+markers',
        name='Tiempo Total', 
        line=dict(color='#45B7D1')
    ))
    
    fig.update_layout(
        title='Tiempos Promedio por Semana',
        xaxis_title='Número de Semana',
        yaxis_title='Tiempo (minutos)',
        hovermode='x unified'
    )
    
    # Set x-axis tick interval to 1
    fig.update_xaxes(dtick=1)
    
    return fig

def create_weekly_delay_chart(weekly_data):
    """Create chart for weekly delay metrics"""
    if weekly_data.empty:
        return None
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=weekly_data['numero_de_semana'],
        y=weekly_data['Tiempo_retraso'],
        mode='lines+markers',
        name='Tiempo de Retraso',
        line=dict(color='#E74C3C')
    ))
    
    # Add zero line
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    
    fig.update_layout(
        title='Tiempo de Retraso Promedio por Semana',
        xaxis_title='Número de Semana',
        yaxis_title='Tiempo (minutos)',
        hovermode='x unified',
        xaxis=dict(dtick=1)
    )
    
    return fig

def create_hourly_times_chart(hourly_data):
    """Create chart for hourly time metrics"""
    if hourly_data.empty:
        return None
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=hourly_data['hora_de_reserva'],
        y=hourly_data['Tiempo_espera'],
        name='Tiempo de Espera',
        marker_color='#FF6B6B'
    ))
    
    fig.add_trace(go.Bar(
        x=hourly_data['hora_de_reserva'],
        y=hourly_data['Tiempo_atencion'],
        name='Tiempo de Atención',
        marker_color='#4ECDC4'
    ))
    
    fig.add_trace(go.Bar(
        x=hourly_data['hora_de_reserva'],
        y=hourly_data['Tiempo_total'],
        name='Tiempo Total',
        marker_color='#45B7D1'
    ))
    
    fig.update_layout(
        title='Tiempos Promedio por Hora de Reserva',
        xaxis_title='Hora de Reserva',
        yaxis_title='Tiempo (minutos)',
        barmode='group'
    )
    
    return fig

def create_hourly_delay_chart(hourly_data):
    """Create chart for hourly delay metrics"""
    if hourly_data.empty:
        return None
    
    fig = go.Figure()
    
    # Color bars based on positive/negative delay
    colors = ['#E74C3C' if x >= 0 else '#27AE60' for x in hourly_data['Tiempo_retraso']]
    
    fig.add_trace(go.Bar(
        x=hourly_data['hora_de_reserva'],
        y=hourly_data['Tiempo_retraso'],
        name='Tiempo de Retraso',
        marker_color=colors
    ))
    
    # Add zero line
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    
    fig.update_layout(
        title='Tiempo de Retraso Promedio por Hora de Reserva',
        xaxis_title='Hora de Reserva',
        yaxis_title='Tiempo (minutos)'
    )
    
    return fig

# ─────────────────────────────────────────────────────────────
# 5. Management Functions - UPDATED FOR GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────
def get_existing_arrivals(gestion_df):
    """Get orders that already have arrival registered today but not yet completed"""
    today = get_bolivia_today().strftime('%Y-%m-%d')
    if gestion_df.empty:
        return []

    if 'Orden_de_compra' in gestion_df.columns:
        gestion_df['Orden_de_compra'] = gestion_df['Orden_de_compra'].astype(str)
 

    
    # Filter records with arrival time from today
    today_arrivals = gestion_df[
        gestion_df['Hora_llegada'].astype(str).str.contains(today, na=False)
    ]
    
    # Only return orders that don't have service times completed
    pending_service = today_arrivals[
        (today_arrivals['Hora_inicio_atencion'].isna()) | 
        (today_arrivals['Hora_inicio_atencion'].astype(str).isin(['', 'nan', 'None'])) |
        (today_arrivals['Hora_fin_atencion'].isna()) |
        (today_arrivals['Hora_fin_atencion'].astype(str).isin(['', 'nan', 'None']))
    ]
    
    return sorted(pending_service['Orden_de_compra'].astype(str).tolist())

def get_completed_orders(gestion_df):
    """Get orders that have both arrival and service registered today"""
    today = get_bolivia_today().strftime('%Y-%m-%d')
    if gestion_df.empty:
        return []
    
    # Filter records with arrival time from today
    today_records = gestion_df[
        gestion_df['Hora_llegada'].astype(str).str.contains(today, na=False)
    ]
    
    # Return orders that have both arrival and service times
    completed = today_records[
        (today_records['Hora_inicio_atencion'].notna()) & 
        (~today_records['Hora_inicio_atencion'].astype(str).isin(['', 'nan', 'None'])) &
        (today_records['Hora_fin_atencion'].notna()) &
        (~today_records['Hora_fin_atencion'].astype(str).isin(['', 'nan', 'None']))
    ]
    
    return completed['Orden_de_compra'].astype(str).tolist()

def get_pending_arrivals(today_reservations, gestion_df):
    """Get orders that haven't registered arrival yet"""
    existing_arrivals = get_existing_arrivals(gestion_df)
    completed_orders = get_completed_orders(gestion_df)
    
    # Combine both lists to exclude from dropdown
    processed_orders = existing_arrivals + completed_orders
    
    # Return orders that haven't been processed at all
    pending = today_reservations[
        ~today_reservations['Orden_de_compra'].isin(processed_orders)
    ]
    
    return sorted(pending['Orden_de_compra'].astype(str).tolist())

def get_arrival_record(gestion_df, orden_compra):
    """Get existing arrival record for an order"""
    if gestion_df.empty:
        return None
    
    # Ensure both sides are strings and strip whitespace
    orden_compra_clean = str(orden_compra).strip()
    
    # Create a mask for exact string matching
    mask = gestion_df['Orden_de_compra'].astype(str).str.strip() == orden_compra_clean
    
    # Filter the dataframe
    matching_records = gestion_df[mask]
    
    if matching_records.empty:
        # Debug: show what we're looking for vs what exists
        available_orders = gestion_df['Orden_de_compra'].astype(str).str.strip().tolist()
        st.error(f"No se encontró orden '{orden_compra_clean}' en registros de gestión.")
        st.error(f"Órdenes disponibles: {available_orders}")
        return None
    
    return matching_records.iloc[0]
def get_arrival_record_silent(gestion_df, orden_compra):
    """Get existing arrival record for an order - silent version without error messages"""
    if gestion_df.empty:
        return None
    
    # Ensure both sides are strings and strip whitespace
    orden_compra_clean = str(orden_compra).strip()
    
    # Create a mask for exact string matching
    mask = gestion_df['Orden_de_compra'].astype(str).str.strip() == orden_compra_clean
    
    # Filter the dataframe
    matching_records = gestion_df[mask]
    
    if matching_records.empty:
        return None
    
    return matching_records.iloc[0]

def save_arrival_to_sheets(arrival_data):
    """Save arrival data to Google Sheets - REPLACES SharePoint Excel save"""
    try:
        credentials_df, reservas_df, gestion_df = download_sheets_to_memory()
        
        if reservas_df is None:
            return False
        
        # Check if record already exists
        existing_record = get_arrival_record_silent(gestion_df, arrival_data['Orden_de_compra'])
        
        if existing_record is not None:
            # Update existing record
            update_data = {
                'Hora_llegada': arrival_data['Hora_llegada'],
                'numero_de_semana': arrival_data['numero_de_semana'],
                'hora_de_reserva': arrival_data['hora_de_reserva'],
                'Tiempo_retraso': arrival_data['Tiempo_retraso']
            }
            return update_sheets_record(arrival_data['Orden_de_compra'], update_data)
        else:
            # Add new record
            return save_gestion_to_sheets(arrival_data)
        
    except Exception as e:
        st.error(f"Error guardando llegada: {str(e)}")
        return False

def update_service_times(orden_compra, service_data):
    """Update service times for existing arrival record - UPDATED FOR GOOGLE SHEETS"""
    try:
        credentials_df, reservas_df, gestion_df = download_sheets_to_memory()
        
        if gestion_df.empty:
            st.error("No hay datos en la hoja de gestión.")
            return False
        
        # Clean the orden_compra for matching
        orden_compra_clean = str(orden_compra).strip()
        
        # Find the record to update with robust string matching
        mask = gestion_df['Orden_de_compra'].astype(str).str.strip() == orden_compra_clean
        matching_records = gestion_df[mask]
        
        if matching_records.empty:
            st.error(f"No se encontró registro de llegada para la orden: '{orden_compra_clean}'")
            available_orders = gestion_df['Orden_de_compra'].astype(str).str.strip().tolist()
            st.error(f"Órdenes disponibles: {available_orders}")
            return False
        
        # Update service times using Google Sheets update function
        return update_sheets_record(orden_compra_clean, service_data)
        
    except Exception as e:
        st.error(f"Error actualizando tiempos de atención: {str(e)}")
        return False

# ─────────────────────────────────────────────────────────────
# 6. Main App - UPDATED FOR GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────
def main():
    st.title("🚚 Control de Proveedores")
    
    # Manual refresh button - rightmost position
    col1, col2 = st.columns([4, 1])
    with col2:
        if st.button("🔄 Actualizar Datos", help="Descargar datos frescos"):
            download_sheets_to_memory.clear()
            st.success("✅ Datos actualizados!")
            st.rerun()
    
    st.markdown("---")
    
    # Load data
    with st.spinner("Cargando datos..."):
        credentials_df, reservas_df, gestion_df = download_sheets_to_memory()
    
    if reservas_df is None:
        st.error("No se pudo cargar los datos. Verifique la conexión.")
        if st.button("🔄 Reintentar Conexión"):
            download_sheets_to_memory.clear()
            st.rerun()
        return
    
    
    # Create tabs with enhanced styling
    tab1, tab2, tab3 = st.tabs(["🚚 REGISTRO DE LLEGADA", "⚙️ REGISTRO DE ATENCIÓN", "📊 DASHBOARD"])
    
    # Visual separator
    st.markdown('<div class="tab-separator"></div>', unsafe_allow_html=True)
    
    # Get today's reservations
    today_reservations = get_today_reservations(reservas_df)
    
    # Check if there are reservations for today (for tabs 1 and 2 only)
    no_reservations_today = today_reservations.empty
    
    # Get order status (only if there are reservations)
    if not no_reservations_today:
        existing_arrivals_orders = get_existing_arrivals(gestion_df)
        completed_orders = get_completed_orders(gestion_df)
        pending_arrivals_orders = get_pending_arrivals(today_reservations, gestion_df)
        
        # Create sorted pending arrivals with proveedor names
        if pending_arrivals_orders:
            pending_reservations = today_reservations[
                today_reservations['Orden_de_compra'].isin(pending_arrivals_orders)
            ].copy()
            
            # Extract first time from Hora column for proper sorting
            def extract_first_time(hora_str):
                """Extract the first time from hora string for sorting"""
                try:
                    hora_str = str(hora_str).strip()
                    # If it contains comma, take the first part
                    if ',' in hora_str:
                        first_time = hora_str.split(',')[0].strip()
                    else:
                        first_time = hora_str
                    
                    # Remove seconds if present and convert to time for sorting
                    if first_time.count(':') == 2:
                        first_time = ':'.join(first_time.split(':')[:2])
                    
                    # Convert to time object for proper sorting
                    from datetime import datetime
                    return datetime.strptime(first_time, '%H:%M').time()
                except:
                    # Fallback: return a default time if parsing fails
                    return datetime.strptime('23:59', '%H:%M').time()
            
            # Add a sort key column
            pending_reservations['sort_time'] = pending_reservations['Hora'].apply(extract_first_time)
            
            # Sort by the time column ascending
            pending_reservations = pending_reservations.sort_values('sort_time')
            
            # Remove the temporary sort column
            pending_reservations = pending_reservations.drop('sort_time', axis=1)
            
            # Create display options: "Proveedor - Orden_de_compra"
            pending_arrivals_display = []
            pending_arrivals_mapping = {}
            
            for _, row in pending_reservations.iterrows():
                display_text = f"{row['Proveedor']} - {row['Orden_de_compra']}"
                pending_arrivals_display.append(display_text)
                pending_arrivals_mapping[display_text] = row['Orden_de_compra']
        else:
            pending_arrivals_display = []
            pending_arrivals_mapping = {}
        
        # Create sorted existing arrivals with proveedor names (for TAB 2)
        if existing_arrivals_orders:
            # Get arrival records with hora_llegada for sorting
            existing_records = gestion_df[
                gestion_df['Orden_de_compra'].astype(str).str.strip().isin([str(x).strip() for x in existing_arrivals_orders])
            ].copy()
            
            # Convert Hora_llegada to datetime for proper sorting
            def convert_to_datetime(datetime_str):
                """Convert datetime string to datetime object for sorting"""
                try:
                    return parse_datetime_flexible(datetime_str)
                except:
                    # Fallback: return a very late time if parsing fails
                    from datetime import datetime
                    return datetime(2099, 12, 31, 23, 59, 59)
            
            existing_records['sort_datetime'] = existing_records['Hora_llegada'].apply(convert_to_datetime)
            
            # Sort by the datetime column ascending
            existing_records = existing_records.sort_values('sort_datetime')
            
            # Remove the temporary sort column
            existing_records = existing_records.drop('sort_datetime', axis=1)
            
            # Create display options: "Proveedor - Orden_de_compra"
            existing_arrivals_display = []
            existing_arrivals_mapping = {}
            
            for _, row in existing_records.iterrows():
                display_text = f"{row['Proveedor']} - {row['Orden_de_compra']}"
                existing_arrivals_display.append(display_text)
                existing_arrivals_mapping[display_text] = row['Orden_de_compra']
        else:
            existing_arrivals_display = []
            existing_arrivals_mapping = {}
    else:
        existing_arrivals = []
        completed_orders = []
        pending_arrivals_display = []
        pending_arrivals_mapping = {}
        existing_arrivals_display = []
        existing_arrivals_mapping = {}

        
    
    # ─────────────────────────────────────────────────────────────
    # TAB 1: Arrival Registration - UPDATED FOR GOOGLE SHEETS
    # ─────────────────────────────────────────────────────────────
    with tab1:
        st.markdown("*Registre la hora de llegada del proveedor*")
        
        if no_reservations_today:
            st.warning("No hay reservas programadas para hoy.")
        else:
            col1, col2 = st.columns(2)
            
            with col1:
                # Order selection - only show orders that haven't been processed
                if not pending_arrivals_display:
                    st.info("✅ Todas las llegadas del día han sido registradas")
                    selected_order_tab1 = None
                    selected_display_tab1 = None
                else:
                    selected_display_tab1 = st.selectbox(
                        "Orden de Compra:",
                        options=pending_arrivals_display,
                        key="order_select_tab1"
                    )

                selected_order_tab1 = pending_arrivals_mapping.get(selected_display_tab1)
                
                if selected_order_tab1:
                    # Get order details
                    order_details = today_reservations[
                        today_reservations['Orden_de_compra'] == selected_order_tab1
                    ].iloc[0]
                    
                    # Auto-fill fields
                    st.text_input(
                        "Proveedor:",
                        value=order_details['Proveedor'],
                        disabled=True
                    )
                    
                    st.text_input(
                        "Número de Bultos:",
                        value=str(order_details['Numero_de_bultos']),
                        disabled=True
                    )
            
            with col2:
                if selected_order_tab1:
                    # Arrival time input with friendly UI
                    st.write("**Hora de Llegada:**")
                    today_date = get_bolivia_today()
                    
                    # Get default time from booked hour in reservations
                    order_details = today_reservations[
                        today_reservations['Orden_de_compra'] == selected_order_tab1
                    ].iloc[0]
                    
                    # Parse the reserved time from the Hora column - UNCHANGED LOGIC
                    hora_str = str(order_details['Hora']).strip()
                    booked_start_time = parse_combined_time_slots(hora_str)
                    if not booked_start_time:
                        booked_start_time = parse_single_time(hora_str)
                    if not booked_start_time:
                        booked_start_time = parse_time_range(hora_str)
                    
                    # Set default hour and minute based on reserved time
                    if booked_start_time:
                        default_hour = booked_start_time.hour
                        default_minute = booked_start_time.minute
                    else:
                        # Fallback: try to extract hour and minute manually
                        try:
                            if ':' in hora_str:
                                time_parts = hora_str.split(':')
                                default_hour = int(time_parts[0])
                                default_minute = int(time_parts[1]) if len(time_parts) > 1 else 0
                            else:
                                # If all parsing fails, use current time
                                current_time = get_bolivia_now()
                                default_hour = max(9, min(18, current_time.hour))
                                default_minute = 0
                        except:
                            # Final fallback
                            current_time = get_bolivia_now()
                            default_hour = max(9, min(18, current_time.hour))
                            default_minute = 0
                    
                    # Ensure hour is within working range
                    default_hour = max(9, min(18, default_hour))
                    # Ensure minute is within valid range
                    default_minute = max(0, min(59, default_minute))
                    
                    # Create user-friendly time picker
                    time_col1, time_col2 = st.columns(2)
                    with time_col1:
                        working_hours = list(range(9, 19))  # 09, 10, 11, 12, 13, 14, 15, 16, 17, 18
                        # Find the index for default hour
                        try:
                            hour_index = working_hours.index(default_hour)
                        except ValueError:
                            hour_index = 0  # Default to first option if not in range
                        
                        arrival_hour = st.selectbox(
                            "Hora:",
                            options=working_hours,
                            index=hour_index,
                            format_func=lambda x: f"{x:02d}",
                            key=f"arrival_hour_tab1_{selected_display_tab1}"
                        )
                    
                    with time_col2:
                        arrival_minute = st.selectbox(
                            "Minutos:",
                            options=list(range(0, 60, 1)),  # 1-minute intervals
                            index=default_minute,  # Direct minute value as index
                            format_func=lambda x: f"{x:02d}",
                            key=f"arrival_minute_tab1_{selected_display_tab1}"
                        )
                    
                    # Combine into time object
                    arrival_time = dt_time(arrival_hour, arrival_minute)
                    
                    st.info(f"Fecha: {today_date.strftime('%Y-%m-%d')}")
                else:
                    # When no order is selected, set arrival_time to None
                    arrival_time = None
            
            # Save arrival button - only show when order is selected
            if selected_order_tab1:
                if st.button("Guardar Llegada", type="primary", key="save_arrival"):
                    if arrival_time:
                        # Get order details for delay calculation
                        order_details = today_reservations[
                            today_reservations['Orden_de_compra'] == selected_order_tab1
                        ].iloc[0]
                        
                        arrival_datetime = combine_date_time(get_bolivia_today().date(), arrival_time)
                        
                        # Calculate delay and extract reservation hour - UNCHANGED LOGIC
                        tiempo_retraso = 0  # Default to 0 if can't calculate
                        hora_de_reserva = None
                        
                        # Get the actual time value from reservations
                        hora_str = str(order_details['Hora']).strip()
                        
                        # Try parsing as combined slots first, then single time, then range
                        booked_start_time = parse_combined_time_slots(hora_str)
                        if not booked_start_time:
                            booked_start_time = parse_single_time(hora_str)
                        if not booked_start_time:
                            booked_start_time = parse_time_range(hora_str)
                        
                        if booked_start_time:
                            booked_datetime = combine_date_time(get_bolivia_today(), booked_start_time)
                            calculated_delay = calculate_time_difference(booked_datetime, arrival_datetime)
                            if calculated_delay is not None:
                                tiempo_retraso = calculated_delay
                            # Extract hour for hora_de_reserva (e.g., 10 for "10:00:00")
                            hora_de_reserva = booked_start_time.hour
                        else:
                            # Fallback: manual calculation for formats like "10:00:00"
                            try:
                                if ':' in hora_str:
                                    time_parts = hora_str.split(':')
                                    booked_hour = int(time_parts[0])
                                    booked_minute = int(time_parts[1]) if len(time_parts) > 1 else 0
                                    booked_second = int(time_parts[2]) if len(time_parts) > 2 else 0
                                    
                                    # Create booked datetime manually
                                    booked_datetime = datetime.combine(
                                        get_bolivia_today(), 
                                        dt_time(booked_hour, booked_minute, booked_second)
                                    )
                                    
                                    # Calculate delay manually
                                    tiempo_retraso = calculate_time_difference(booked_datetime, arrival_datetime)
                                    hora_de_reserva = booked_hour
                            except Exception:
                                # If all else fails, set to defaults
                                hora_de_reserva = None
                                tiempo_retraso = 0
                        
                        # Prepare arrival data - MAINTAIN EXACT DATE FORMAT
                        arrival_data = {
                            'Orden_de_compra': selected_order_tab1,
                            'Proveedor': order_details['Proveedor'],
                            'Numero_de_bultos': order_details['Numero_de_bultos'],
                            'Hora_llegada': format_datetime_no_zero_padding(arrival_datetime),  # EXACT FORMAT
                            'Hora_inicio_atencion': '',
                            'Hora_fin_atencion': '',
                            'Tiempo_espera': '',
                            'Tiempo_atencion': '',
                            'Tiempo_total': '',
                            'Tiempo_retraso': tiempo_retraso,
                            'numero_de_semana': arrival_datetime.isocalendar()[1],
                            'hora_de_reserva': hora_de_reserva
                        }
                        
                        # Save to Google Sheets
                        with st.spinner("Guardando llegada..."):
                            if save_arrival_to_sheets(arrival_data):
                                st.success("✅ Llegada registrada exitosamente!")
                                if tiempo_retraso > 0:
                                    st.warning(f"⏰ Retraso: {tiempo_retraso} minutos")
                                elif tiempo_retraso < 0:
                                    st.info(f"⚡ Adelanto: {abs(tiempo_retraso)} minutos")
                                else:
                                    st.success("🎯 Llegada puntual")
                                
                                # Wait 5 seconds before refreshing
                                with st.spinner("Actualizando datos..."):
                                    time.sleep(5)
                                st.rerun()
                            else:
                                st.error("Error al guardar la llegada. Intente nuevamente.")
                    else:
                        st.error("Por favor complete todos los campos.")
    
    # ─────────────────────────────────────────────────────────────
    # TAB 2: Service Registration - UPDATED FOR GOOGLE SHEETS
    # ─────────────────────────────────────────────────────────────
    with tab2:
        st.markdown("*Registre los tiempos de inicio y fin de atención*")
        
        if no_reservations_today:
            st.warning("No hay reservas programadas para hoy.")
        else:
            # Order selection
            if existing_arrivals_display:
                selected_display_tab2 = st.selectbox(
                    "Orden de Compra:",
                    options=existing_arrivals_display,
                    key="order_select_tab2"
                )
                # Get the actual orden_de_compra from the mapping
                selected_order_tab2 = existing_arrivals_mapping.get(selected_display_tab2)
            else:
                selected_display_tab2 = st.selectbox(
                    "Orden de Compra:",
                    options=["No hay llegadas registradas"],
                    disabled=True,
                    key="order_select_tab2"
                )
                selected_order_tab2 = None
            
            if existing_arrivals_display  and selected_order_tab2:
                # Get arrival record
                arrival_record = get_arrival_record(gestion_df, selected_order_tab2)
                
                if arrival_record is not None:
                    # Show arrival info
                    arrival_time_str = str(arrival_record['Hora_llegada'])
                    st.markdown(f'''
                    <div class="service-info">
                        <strong>Proveedor:</strong> {arrival_record['Proveedor']} | 
                        <strong>Llegada:</strong> {arrival_time_str.split(' ')[1][:5] if ' ' in arrival_time_str else 'N/A'} | 
                        <strong>Número de Bultos:</strong> {arrival_record['Numero_de_bultos']}
                    </div>
                    ''', unsafe_allow_html=True)
                    
                    # Check if service times already registered
                    service_registered = (
                        pd.notna(arrival_record['Hora_inicio_atencion']) and 
                        str(arrival_record['Hora_inicio_atencion']).strip() not in ['', 'nan', 'None'] and
                        pd.notna(arrival_record['Hora_fin_atencion']) and
                        str(arrival_record['Hora_fin_atencion']).strip() not in ['', 'nan', 'None']
                    )
                    
                    if service_registered:
                        st.success("✅ Atención ya registrada")
                        # Show existing times
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Tiempo de Espera", f"{arrival_record['Tiempo_espera']} min")
                            st.metric("Tiempo de Atención", f"{arrival_record['Tiempo_atencion']} min")
                        with col2:
                            st.metric("Tiempo Total", f"{arrival_record['Tiempo_total']} min")
                    else:
                        st.warning("⏳ Pendiente de registrar atención")
                        
                        # Service time inputs - only show when not registered
                        col1, col2 = st.columns(2)
                        
                        # Parse arrival time for defaults
                        arrival_datetime = parse_datetime_flexible(str(arrival_record['Hora_llegada']))
                        # Ensure default hour is within service hours (9-18)
                        default_hour = max(9, min(18, arrival_datetime.hour))
                        default_minute = arrival_datetime.minute  # Use exact minute instead of rounding
                        
                        with col1:
                            st.write("**Hora de Inicio de Atención:**")
                            
                            start_time_col1, start_time_col2 = st.columns(2)
                            with start_time_col1:
                                service_hours = list(range(9, 19))  # 09, 10, 11, 12, 13, 14, 15, 16, 17, 18
                                # Find the index for default hour
                                try:
                                    start_hour_index = service_hours.index(default_hour)
                                except ValueError:
                                    start_hour_index = 0  # Default to first option if not in range
                                
                                start_hour = st.selectbox(
                                    "Hora:",
                                    options=service_hours,
                                    index=start_hour_index,
                                    format_func=lambda x: f"{x:02d}",
                                    key=f"start_hour_tab2_{selected_display_tab2}"
                                )
                            
                            with start_time_col2:
                                start_minute = st.selectbox(
                                    "Minutos:",
                                    options=list(range(0, 60, 1)),  # 1-minute intervals
                                    index=default_minute,  # Direct minute value
                                    format_func=lambda x: f"{x:02d}",
                                    key=f"start_minute_tab2_{selected_display_tab2}"
                                )
                            
                            start_time = dt_time(start_hour, start_minute)
                        
                        with col2:
                            st.write("**Hora de Fin de Atención:**")
                            
                            end_time_col1, end_time_col2 = st.columns(2)
                            with end_time_col1:
                                service_hours = list(range(9, 19))  # 09, 10, 11, 12, 13, 14, 15, 16, 17, 18
                                # Find the index for default hour
                                try:
                                    end_hour_index = service_hours.index(default_hour)
                                except ValueError:
                                    end_hour_index = 0  # Default to first option if not in range
                                
                                end_hour = st.selectbox(
                                    "Hora:",
                                    options=service_hours,
                                    index=end_hour_index,
                                    format_func=lambda x: f"{x:02d}",
                                    key=f"end_hour_tab2_{selected_display_tab2}"
                                )
                            
                            with end_time_col2:
                                end_minute = st.selectbox(
                                    "Minutos:",
                                    options=list(range(0, 60, 1)),  # 1-minute intervals
                                    index=default_minute,  # Direct minute value
                                    format_func=lambda x: f"{x:02d}",
                                    key=f"end_minute_tab2_{selected_display_tab2}"
                                )
                            
                            end_time = dt_time(end_hour, end_minute)
                        
                        # Save service times button - only show when not registered
                        if st.button("Guardar Atención", type="primary", key="save_service"):

                            
                            if start_time and end_time:
                                today_date = get_bolivia_today()
                                hora_inicio = combine_date_time(today_date, start_time)
                                hora_fin = combine_date_time(today_date, end_time)
                                
                                # Parse arrival time
                                arrival_datetime = parse_datetime_flexible(str(arrival_record['Hora_llegada']))
                                
                                # Validate times - UNCHANGED LOGIC
                                if hora_inicio >= hora_fin:
                                    st.error("La hora de fin debe ser posterior a la hora de inicio.")
                                elif hora_inicio < arrival_datetime:
                                    st.error("La hora de inicio de atención no puede ser anterior a la hora de llegada.")
                                else:
                                    # Calculate times - UNCHANGED LOGIC
                                    tiempo_espera = calculate_time_difference(arrival_datetime, hora_inicio)
                                    tiempo_atencion = calculate_time_difference(hora_inicio, hora_fin)
                                    tiempo_total = calculate_time_difference(arrival_datetime, hora_fin)
                                    
                                    # Prepare service data - MAINTAIN EXACT DATE FORMAT
                                    service_data = {
                                        'Hora_inicio_atencion': format_datetime_no_zero_padding(hora_inicio),
                                        'Hora_fin_atencion': format_datetime_no_zero_padding(hora_fin),
                                        'Tiempo_espera': tiempo_espera,
                                        'Tiempo_atencion': tiempo_atencion,
                                        'Tiempo_total': tiempo_total
                                    }
                                    
                                    # Save to Google Sheets
                                    with st.spinner("Guardando atención..."):
                                        if update_service_times(selected_order_tab2, service_data):
                                            st.success("✅ Atención registrada exitosamente!")
                                            
                                            # Calculate delay for summary - UNCHANGED LOGIC
                                            arrival_datetime = parse_datetime_flexible(str(arrival_record['Hora_llegada']))
                                            
                                            # Get the booked time from reservas_df
                                            order_reserva = today_reservations[
                                                today_reservations['Orden_de_compra'] == selected_order_tab2
                                            ]
                                            
                                            tiempo_retraso_display = 0  # Default to 0 if can't calculate
                                            if not order_reserva.empty:
                                                booked_time_range = str(order_reserva.iloc[0]['Hora'])
                                                # Try parsing as combined slots first, then single time, then range
                                                booked_start_time = parse_combined_time_slots(booked_time_range)
                                                if not booked_start_time:
                                                    booked_start_time = parse_single_time(booked_time_range)
                                                if not booked_start_time:
                                                    booked_start_time = parse_time_range(booked_time_range)                                                
                                                
                                                if booked_start_time:
                                                    booked_datetime = combine_date_time(arrival_datetime.date(), booked_start_time)
                                                    calculated_delay = calculate_time_difference(booked_datetime, arrival_datetime)
                                                    if calculated_delay is not None:
                                                        tiempo_retraso_display = calculated_delay
                                                else:
                                                    # Fallback: manual calculation for formats like "10:00:00"
                                                    try:
                                                        if ':' in booked_time_range:
                                                            time_parts = booked_time_range.split(':')
                                                            booked_hour = int(time_parts[0])
                                                            booked_minute = int(time_parts[1]) if len(time_parts) > 1 else 0
                                                            booked_second = int(time_parts[2]) if len(time_parts) > 2 else 0
                                                            
                                                            # Create booked datetime manually
                                                            booked_datetime = datetime.combine(
                                                                arrival_datetime.date(), 
                                                                dt_time(booked_hour, booked_minute, booked_second)
                                                            )
                                                            
                                                            # Calculate delay manually
                                                            tiempo_retraso_display = calculate_time_difference(booked_datetime, arrival_datetime)
                                                    except Exception:
                                                        # Keep default value of 0
                                                        pass
                                            
                                            # Show summary
                                            col1, col2 = st.columns(2)
                                            with col1:
                                                st.metric("Tiempo de Espera", f"{tiempo_espera} min")
                                                st.metric("Tiempo de Atención", f"{tiempo_atencion} min")
                                            with col2:
                                                st.metric("Tiempo Total", f"{tiempo_total} min")
                                                # Display calculated delay
                                                if tiempo_retraso_display > 0:
                                                    st.metric("Tiempo de Retraso", f"{tiempo_retraso_display} min")
                                                elif tiempo_retraso_display < 0:
                                                    st.metric("Tiempo de Adelanto", f"{abs(tiempo_retraso_display)} min")
                                                else:
                                                    st.metric("Tiempo de Retraso", f"{tiempo_retraso_display} min")
                                            
                                            # Wait 10 seconds before refreshing
                                            with st.spinner("Actualizando datos..."):
                                                time.sleep(10)
                                            st.rerun()
                                        else:
                                            st.error("Error al guardar la atención. Intente nuevamente.")
                            else:
                                st.error("Por favor complete todos los campos de tiempo.")
            else:
                st.markdown(
                    '<div class="service-info">⚠️ No hay llegadas registradas hoy. Primero debe registrar la llegada en la pestaña anterior.</div>', 
                    unsafe_allow_html=True
                )
    
    # ─────────────────────────────────────────────────────────────
    # TAB 3: Dashboard - UNCHANGED LOGIC
    # ─────────────────────────────────────────────────────────────
    with tab3:
        st.markdown("*Análisis y tendencias de rendimiento de proveedores*")
        
        # Check if we have data
        if gestion_df.empty:
            st.warning("📊 No hay datos disponibles para mostrar gráficos.")
            return
        
        # Filter controls
        st.subheader("🔧 Controles de Filtrado")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Provider filter
            providers = ["Todos"] + sorted(gestion_df['Proveedor'].dropna().unique().tolist())
            selected_provider = st.selectbox(
                "Proveedor:",
                options=providers,
                key="dashboard_provider"
            )
        
        with col2:
            # Week range filter
            week_options = {
                "1 semana": 1,
                "2 semanas": 2, 
                "4 semanas": 4,
                "12 semanas": 12,
                "24 semanas": 24
            }
            selected_weeks_label = st.selectbox(
                "Período (semanas completas):",
                options=list(week_options.keys()),
                key="dashboard_weeks"
            )
            selected_weeks = week_options[selected_weeks_label]
        
        st.markdown("---")
        
        # Get filtered data
        filtered_data = get_completed_weeks_data(gestion_df, selected_weeks)
        
        # Display number of entries being used for dashboard
        stats_data_count = filtered_data.copy()
        if selected_provider != "Todos":
            stats_data_count = stats_data_count[stats_data_count['Proveedor'] == selected_provider]
        st.caption(f"📊 Mostrando {len(stats_data_count)} registros para el análisis")

        
        if filtered_data.empty:
            st.warning(f"📊 No hay datos completos para las últimas {selected_weeks} semanas.")
            return
        
        # Summary stats - MOVED TO BEGINNING
        st.subheader("📊 Estadísticas del Período")
        
        # Filter by provider for stats
        stats_data = filtered_data.copy()
        if selected_provider != "Todos":
            stats_data = stats_data[stats_data['Proveedor'] == selected_provider]
        
        if not stats_data.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            # Convert to numeric for calculations
            for col in ['Tiempo_espera', 'Tiempo_atencion', 'Tiempo_total', 'Tiempo_retraso']:
                stats_data[col] = pd.to_numeric(stats_data[col], errors='coerce')
            
            with col1:
                avg_wait = stats_data['Tiempo_espera'].mean()
                st.metric("Espera Promedio", f"{avg_wait:.1f} min")
            
            with col2:
                avg_service = stats_data['Tiempo_atencion'].mean()
                st.metric("Atención Promedio", f"{avg_service:.1f} min")
            
            with col3:
                avg_total = stats_data['Tiempo_total'].mean()
                st.metric("Total Promedio", f"{avg_total:.1f} min")
            
            with col4:
                avg_delay = stats_data['Tiempo_retraso'].mean()
                st.metric("Retraso Promedio", f"{avg_delay:.1f} min")
        
        st.markdown("---")
        
        # Graph 1: Weekly Time Metrics
        st.subheader("📈 Gráfico 1: Tiempos por Semana")
        weekly_data = aggregate_by_week(filtered_data, selected_provider)
        
        if not weekly_data.empty:
            fig1 = create_weekly_times_chart(weekly_data)
            if fig1:
                st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("No hay datos para el proveedor seleccionado en el período especificado.")
        
        st.markdown("---")
        
        # Graph 2: Weekly Delay Metrics  
        st.subheader("⏰ Gráfico 2: Retrasos por Semana")
        
        if not weekly_data.empty:
            fig2 = create_weekly_delay_chart(weekly_data)
            if fig2:
                st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No hay datos para el proveedor seleccionado en el período especificado.")
        
        st.markdown("---")
        
        # Graph 3: Hourly Time Metrics
        st.subheader("🕐 Gráfico 3: Tiempos por Hora de Reserva")
        hourly_data = aggregate_by_hour_from_filtered(filtered_data, selected_provider)
        
        if not hourly_data.empty:
            fig3 = create_hourly_times_chart(hourly_data)
            if fig3:
                st.plotly_chart(fig3, use_container_width=True)
        else:
            if selected_provider != "Todos":
                st.info(f"No hay datos de horas de reserva para el proveedor {selected_provider} en el período especificado.")
            else:
                st.info("No hay datos de horas de reserva para el período especificado.")
        
        st.markdown("---")
        
        # Graph 4: Hourly Delay Metrics
        st.subheader("⚡ Gráfico 4: Retrasos por Hora de Reserva")
        
        if not hourly_data.empty:
            fig4 = create_hourly_delay_chart(hourly_data)
            if fig4:
                st.plotly_chart(fig4, use_container_width=True)
        else:
            if selected_provider != "Todos":
                st.info(f"No hay datos de horas de reserva para el proveedor {selected_provider} en el período especificado.")
            else:
                st.info("No hay datos de horas de reserva para el período especificado.")

if __name__ == "__main__":
    main()