import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from datetime import datetime
import os

# Configuración de la página
st.set_page_config(
    page_title="Olist E-Commerce Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Ruta a la base de datos
# Ajusta esta ruta según donde esté tu archivo olist.db
SQLITE_BD_PATH = os.path.join(os.path.dirname(__file__), "..", "olist.db")

# Verificar que existe
if not os.path.exists(SQLITE_BD_PATH):
    st.error(f"⚠️ Base de datos no encontrada en: {SQLITE_BD_PATH}")
    st.info("Asegúrate de ejecutar el pipeline de Airflow primero para crear la base de datos.")
    st.stop()

# Función para conectar a la base de datos
@st.cache_resource
def get_database_connection():
    """Crea conexión a la base de datos SQLite"""
    return create_engine(f"sqlite:///{SQLITE_BD_PATH}")

# Función para cargar datos con caché
@st.cache_data
def load_data(query):
    """Carga datos desde la base de datos"""
    engine = get_database_connection()
    return pd.read_sql(query, engine)

# Título principal
st.title("📊 Olist E-Commerce Analytics Dashboard")
st.markdown("### Análisis de Pedidos 2016-2018")

# Sidebar para filtros
st.sidebar.header("Filtros")

# Intentar cargar datos básicos para filtros
try:
    # Query para obtener años disponibles
    years_query = """
    SELECT DISTINCT strftime('%Y', order_purchase_timestamp) as year
    FROM olist_orders
    WHERE order_purchase_timestamp IS NOT NULL
    ORDER BY year
    """
    available_years = load_data(years_query)
    
    if not available_years.empty:
        years = available_years['year'].tolist()
        selected_years = st.sidebar.multiselect(
            "Seleccionar Años",
            options=years,
            default=years
        )
    else:
        selected_years = []
        st.sidebar.warning("No hay datos de años disponibles")

except Exception as e:
    st.sidebar.error(f"Error cargando filtros: {str(e)}")
    selected_years = []

# Crear tabs para diferentes secciones
tab1, tab2, tab3, tab4 = st.tabs(["📈 Ingresos", "🚚 Entregas", "📦 Productos", "⭐ Reviews"])

# ==================== TAB 1: INGRESOS ====================
with tab1:
    st.header("Análisis de Ingresos")
    
    col1, col2, col3, col4 = st.columns(4)
    
    try:
        # KPIs principales
        kpi_query = f"""
        SELECT 
            COUNT(DISTINCT o.order_id) as total_orders,
            ROUND(SUM(oi.price), 2) as total_revenue,
            ROUND(AVG(oi.price), 2) as avg_order_value,
            COUNT(DISTINCT o.customer_id) as total_customers
        FROM olist_orders o
        JOIN olist_order_items oi ON o.order_id = oi.order_id
        WHERE strftime('%Y', o.order_purchase_timestamp) IN ({','.join(["'" + y + "'" for y in selected_years])})
        """
        
        kpis = load_data(kpi_query)
        
        if not kpis.empty:
            with col1:
                st.metric("Total Pedidos", f"{kpis['total_orders'].iloc[0]:,.0f}")
            with col2:
                st.metric("Ingresos Totales", f"R$ {kpis['total_revenue'].iloc[0]:,.2f}")
            with col3:
                st.metric("Valor Promedio", f"R$ {kpis['avg_order_value'].iloc[0]:,.2f}")
            with col4:
                st.metric("Total Clientes", f"{kpis['total_customers'].iloc[0]:,.0f}")
    except Exception as e:
        st.error(f"Error cargando KPIs: {str(e)}")
    
    # Ingresos por año
    st.subheader("Ingresos por Año")
    try:
        revenue_year_query = f"""
        SELECT 
            strftime('%Y', o.order_purchase_timestamp) as year,
            ROUND(SUM(oi.price), 2) as revenue,
            COUNT(DISTINCT o.order_id) as orders
        FROM olist_orders o
        JOIN olist_order_items oi ON o.order_id = oi.order_id
        WHERE strftime('%Y', o.order_purchase_timestamp) IN ({','.join(["'" + y + "'" for y in selected_years])})
        GROUP BY year
        ORDER BY year
        """
        
        revenue_year = load_data(revenue_year_query)
        
        if not revenue_year.empty:
            fig = px.bar(
                revenue_year, 
                x='year', 
                y='revenue',
                title='Ingresos por Año',
                labels={'revenue': 'Ingresos (R$)', 'year': 'Año'},
                color='revenue',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error en ingresos por año: {str(e)}")
    
    # Ingresos por estado y categoría
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Top 10 Estados por Ingresos")
        try:
            revenue_state_query = f"""
            SELECT 
                c.customer_state as state,
                ROUND(SUM(oi.price), 2) as revenue
            FROM olist_orders o
            JOIN olist_order_items oi ON o.order_id = oi.order_id
            JOIN olist_customers c ON o.customer_id = c.customer_id
            WHERE strftime('%Y', o.order_purchase_timestamp) IN ({','.join(["'" + y + "'" for y in selected_years])})
            GROUP BY state
            ORDER BY revenue DESC
            LIMIT 10
            """
            
            revenue_state = load_data(revenue_state_query)
            
            if not revenue_state.empty:
                fig = px.bar(
                    revenue_state,
                    x='revenue',
                    y='state',
                    orientation='h',
                    title='Ingresos por Estado',
                    labels={'revenue': 'Ingresos (R$)', 'state': 'Estado'},
                    color='revenue',
                    color_continuous_scale='Greens'
                )
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error en ingresos por estado: {str(e)}")
    
    with col2:
        st.subheader("Top 10 Categorías por Ingresos")
        try:
            revenue_category_query = f"""
            SELECT 
                p.product_category_name as category,
                ROUND(SUM(oi.price), 2) as revenue,
                COUNT(DISTINCT oi.order_id) as orders
            FROM olist_orders o
            JOIN olist_order_items oi ON o.order_id = oi.order_id
            JOIN olist_products p ON oi.product_id = p.product_id
            WHERE strftime('%Y', o.order_purchase_timestamp) IN ({','.join(["'" + y + "'" for y in selected_years])})
            AND p.product_category_name IS NOT NULL
            GROUP BY category
            ORDER BY revenue DESC
            LIMIT 10
            """
            
            revenue_category = load_data(revenue_category_query)
            
            if not revenue_category.empty:
                fig = px.pie(
                    revenue_category,
                    values='revenue',
                    names='category',
                    title='Distribución de Ingresos por Categoría'
                )
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error en ingresos por categoría: {str(e)}")

# ==================== TAB 2: ENTREGAS ====================
with tab2:
    st.header("Análisis de Entregas")
    
    col1, col2, col3 = st.columns(3)
    
    try:
        # KPIs de entregas
        delivery_kpi_query = f"""
        SELECT 
            ROUND(AVG(julianday(order_delivered_customer_date) - julianday(order_purchase_timestamp)), 1) as avg_delivery_days,
            ROUND(AVG(julianday(order_delivered_customer_date) - julianday(order_estimated_delivery_date)), 1) as avg_delay_days,
            ROUND(100.0 * SUM(CASE WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 1 ELSE 0 END) / COUNT(*), 1) as on_time_pct
        FROM olist_orders
        WHERE order_delivered_customer_date IS NOT NULL
        AND order_estimated_delivery_date IS NOT NULL
        AND strftime('%Y', order_purchase_timestamp) IN ({','.join(["'" + y + "'" for y in selected_years])})
        """
        
        delivery_kpis = load_data(delivery_kpi_query)
        
        if not delivery_kpis.empty:
            with col1:
                st.metric("Días Promedio Entrega", f"{delivery_kpis['avg_delivery_days'].iloc[0]:.1f}")
            with col2:
                delay = delivery_kpis['avg_delay_days'].iloc[0]
                st.metric("Diferencia vs Estimado", f"{delay:+.1f} días")
            with col3:
                st.metric("Entregas a Tiempo", f"{delivery_kpis['on_time_pct'].iloc[0]:.1f}%")
    except Exception as e:
        st.error(f"Error cargando KPIs de entrega: {str(e)}")
    
    # Tiempo de entrega por mes
    st.subheader("Tiempo de Entrega por Mes")
    try:
        delivery_month_query = f"""
        SELECT 
            strftime('%Y-%m', order_purchase_timestamp) as month,
            ROUND(AVG(julianday(order_delivered_customer_date) - julianday(order_purchase_timestamp)), 1) as avg_days
        FROM olist_orders
        WHERE order_delivered_customer_date IS NOT NULL
        AND strftime('%Y', order_purchase_timestamp) IN ({','.join(["'" + y + "'" for y in selected_years])})
        GROUP BY month
        ORDER BY month
        """
        
        delivery_month = load_data(delivery_month_query)
        
        if not delivery_month.empty:
            fig = px.line(
                delivery_month,
                x='month',
                y='avg_days',
                title='Tiempo Promedio de Entrega por Mes',
                labels={'avg_days': 'Días Promedio', 'month': 'Mes'},
                markers=True
            )
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error en entregas por mes: {str(e)}")
    
    # Entregas a tiempo vs retrasadas
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Distribución de Entregas")
        try:
            delivery_status_query = f"""
            SELECT 
                CASE 
                    WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 'A Tiempo'
                    ELSE 'Retrasada'
                END as status,
                COUNT(*) as count
            FROM olist_orders
            WHERE order_delivered_customer_date IS NOT NULL
            AND order_estimated_delivery_date IS NOT NULL
            AND strftime('%Y', order_purchase_timestamp) IN ({','.join(["'" + y + "'" for y in selected_years])})
            GROUP BY status
            """
            
            delivery_status = load_data(delivery_status_query)
            
            if not delivery_status.empty:
                fig = px.pie(
                    delivery_status,
                    values='count',
                    names='status',
                    title='Entregas: A Tiempo vs Retrasadas',
                    color='status',
                    color_discrete_map={'A Tiempo': 'green', 'Retrasada': 'red'}
                )
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error en distribución de entregas: {str(e)}")
    
    with col2:
        st.subheader("Entregas por Estado")
        try:
            delivery_state_query = f"""
            SELECT 
                c.customer_state as state,
                COUNT(*) as orders,
                ROUND(AVG(julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp)), 1) as avg_days
            FROM olist_orders o
            JOIN olist_customers c ON o.customer_id = c.customer_id
            WHERE o.order_delivered_customer_date IS NOT NULL
            AND strftime('%Y', o.order_purchase_timestamp) IN ({','.join(["'" + y + "'" for y in selected_years])})
            GROUP BY state
            ORDER BY orders DESC
            LIMIT 10
            """
            
            delivery_state = load_data(delivery_state_query)
            
            if not delivery_state.empty:
                fig = px.scatter(
                    delivery_state,
                    x='orders',
                    y='avg_days',
                    size='orders',
                    text='state',
                    title='Entregas vs Tiempo por Estado',
                    labels={'orders': 'Número de Pedidos', 'avg_days': 'Días Promedio'}
                )
                fig.update_traces(textposition='top center')
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error en entregas por estado: {str(e)}")

# ==================== TAB 3: PRODUCTOS ====================
with tab3:
    st.header("Análisis de Productos")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Top 10 Productos Más Vendidos")
        try:
            top_products_query = f"""
            SELECT 
                p.product_category_name as category,
                COUNT(oi.order_id) as sales,
                ROUND(SUM(oi.price), 2) as revenue
            FROM olist_order_items oi
            JOIN olist_products p ON oi.product_id = p.product_id
            JOIN olist_orders o ON oi.order_id = o.order_id
            WHERE strftime('%Y', o.order_purchase_timestamp) IN ({','.join(["'" + y + "'" for y in selected_years])})
            AND p.product_category_name IS NOT NULL
            GROUP BY category
            ORDER BY sales DESC
            LIMIT 10
            """
            
            top_products = load_data(top_products_query)
            
            if not top_products.empty:
                fig = px.bar(
                    top_products,
                    x='sales',
                    y='category',
                    orientation='h',
                    title='Categorías Más Vendidas',
                    labels={'sales': 'Ventas', 'category': 'Categoría'},
                    color='sales',
                    color_continuous_scale='Purples'
                )
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error en productos más vendidos: {str(e)}")
    
    with col2:
        st.subheader("Top 10 Productos Menos Vendidos")
        try:
            bottom_products_query = f"""
            SELECT 
                p.product_category_name as category,
                COUNT(oi.order_id) as sales,
                ROUND(SUM(oi.price), 2) as revenue
            FROM olist_order_items oi
            JOIN olist_products p ON oi.product_id = p.product_id
            JOIN olist_orders o ON oi.order_id = o.order_id
            WHERE strftime('%Y', o.order_purchase_timestamp) IN ({','.join(["'" + y + "'" for y in selected_years])})
            AND p.product_category_name IS NOT NULL
            GROUP BY category
            ORDER BY sales ASC
            LIMIT 10
            """
            
            bottom_products = load_data(bottom_products_query)
            
            if not bottom_products.empty:
                fig = px.bar(
                    bottom_products,
                    x='sales',
                    y='category',
                    orientation='h',
                    title='Categorías Menos Vendidas',
                    labels={'sales': 'Ventas', 'category': 'Categoría'},
                    color='sales',
                    color_continuous_scale='Reds'
                )
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error en productos menos vendidos: {str(e)}")
    
    # Precio promedio por categoría
    st.subheader("Precio Promedio por Categoría")
    try:
        avg_price_query = f"""
        SELECT 
            p.product_category_name as category,
            ROUND(AVG(oi.price), 2) as avg_price,
            COUNT(oi.order_id) as orders
        FROM olist_order_items oi
        JOIN olist_products p ON oi.product_id = p.product_id
        JOIN olist_orders o ON oi.order_id = o.order_id
        WHERE strftime('%Y', o.order_purchase_timestamp) IN ({','.join(["'" + y + "'" for y in selected_years])})
        AND p.product_category_name IS NOT NULL
        GROUP BY category
        HAVING orders > 50
        ORDER BY avg_price DESC
        LIMIT 15
        """
        
        avg_price = load_data(avg_price_query)
        
        if not avg_price.empty:
            fig = px.bar(
                avg_price,
                x='avg_price',
                y='category',
                orientation='h',
                title='Precio Promedio por Categoría (>50 pedidos)',
                labels={'avg_price': 'Precio Promedio (R$)', 'category': 'Categoría'},
                color='avg_price',
                color_continuous_scale='Oranges'
            )
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error en precio promedio: {str(e)}")

# ==================== TAB 4: REVIEWS ====================
with tab4:
    st.header("Análisis de Reviews")
    
    col1, col2, col3 = st.columns(3)
    
    try:
        # KPIs de reviews
        review_kpi_query = f"""
        SELECT 
            ROUND(AVG(review_score), 2) as avg_score,
            COUNT(*) as total_reviews,
            ROUND(100.0 * SUM(CASE WHEN review_score >= 4 THEN 1 ELSE 0 END) / COUNT(*), 1) as positive_pct
        FROM olist_order_reviews r
        JOIN olist_orders o ON r.order_id = o.order_id
        WHERE strftime('%Y', o.order_purchase_timestamp) IN ({','.join(["'" + y + "'" for y in selected_years])})
        """
        
        review_kpis = load_data(review_kpi_query)
        
        if not review_kpis.empty:
            with col1:
                st.metric("Calificación Promedio", f"{review_kpis['avg_score'].iloc[0]:.2f} ⭐")
            with col2:
                st.metric("Total Reviews", f"{review_kpis['total_reviews'].iloc[0]:,.0f}")
            with col3:
                st.metric("Reviews Positivas", f"{review_kpis['positive_pct'].iloc[0]:.1f}%")
    except Exception as e:
        st.error(f"Error cargando KPIs de reviews: {str(e)}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Distribución de Calificaciones")
        try:
            score_dist_query = f"""
            SELECT 
                review_score as score,
                COUNT(*) as count
            FROM olist_order_reviews r
            JOIN olist_orders o ON r.order_id = o.order_id
            WHERE strftime('%Y', o.order_purchase_timestamp) IN ({','.join(["'" + y + "'" for y in selected_years])})
            GROUP BY score
            ORDER BY score
            """
            
            score_dist = load_data(score_dist_query)
            
            if not score_dist.empty:
                fig = px.bar(
                    score_dist,
                    x='score',
                    y='count',
                    title='Distribución de Calificaciones',
                    labels={'count': 'Cantidad', 'score': 'Calificación'},
                    color='score',
                    color_continuous_scale='RdYlGn'
                )
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error en distribución de calificaciones: {str(e)}")
    
    with col2:
        st.subheader("Calificación por Categoría")
        try:
            category_score_query = f"""
            SELECT 
                p.product_category_name as category,
                ROUND(AVG(r.review_score), 2) as avg_score,
                COUNT(*) as reviews
            FROM olist_order_reviews r
            JOIN olist_orders o ON r.order_id = o.order_id
            JOIN olist_order_items oi ON o.order_id = oi.order_id
            JOIN olist_products p ON oi.product_id = p.product_id
            WHERE strftime('%Y', o.order_purchase_timestamp) IN ({','.join(["'" + y + "'" for y in selected_years])})
            AND p.product_category_name IS NOT NULL
            GROUP BY category
            HAVING reviews > 20
            ORDER BY avg_score DESC
            LIMIT 15
            """
            
            category_score = load_data(category_score_query)
            
            if not category_score.empty:
                fig = px.bar(
                    category_score,
                    x='avg_score',
                    y='category',
                    orientation='h',
                    title='Calificación Promedio por Categoría (>20 reviews)',
                    labels={'avg_score': 'Calificación Promedio', 'category': 'Categoría'},
                    color='avg_score',
                    color_continuous_scale='RdYlGn'
                )
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error en calificación por categoría: {str(e)}")

# Footer
st.markdown("---")
st.markdown("Dashboard creado con Streamlit | Datos: Olist E-Commerce Dataset 2016-2018")