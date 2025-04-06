import dash
from dash import dcc, html, dash_table, Input, Output, State, callback, ctx, ALL, DiskcacheManager # Use DiskcacheManager for callbacks
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import json
import os
from collections import defaultdict
import uuid # For unique IDs in pattern matching callbacks if needed

# --- Local Module Imports ---
from data import query as data_query # Use alias for clarity

# --- Configuration ---
APP_TITLE = "Database Spend Dashboard"
BOOKMARK_FILE = "bookmarks.json"
TABLE_PAGE_SIZE = 30

# Initialize Dash App
# Use DiskcacheManager for caching callback outputs if needed, can help with large data
# import diskcache
# cache = diskcache.Cache("./cache")
# background_callback_manager = DiskcacheManager(cache)
# app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], background_callback_manager=background_callback_manager)

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = APP_TITLE
server = app.server # Expose server for deployment

# --- Helper Functions ---

def func_load_bookmarks():
    """Loads bookmarks from the JSON file."""
    if os.path.exists(BOOKMARK_FILE):
        try:
            with open(BOOKMARK_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: Bookmark file '{BOOKMARK_FILE}' is corrupted. Starting fresh.")
            return {}
        except Exception as e:
            print(f"Error loading bookmarks: {e}")
            return {}
    return {}

def func_save_bookmarks(p_dict_bookmarks):
    """Saves bookmarks to the JSON file."""
    try:
        with open(BOOKMARK_FILE, 'w') as f:
            json.dump(p_dict_bookmarks, f, indent=4)
    except Exception as e:
        print(f"Error saving bookmarks: {e}")

def func_get_bookmark_options(p_dict_bookmarks):
    """Generates dropdown options from bookmark dictionary."""
    return [{'label': name, 'value': name} for name in sorted(p_dict_bookmarks.keys())]

def func_create_empty_figure(p_str_title=""):
    """Creates an empty Plotly figure with dark theme."""
    fig = go.Figure()
    fig.update_layout(
        title=dict(text=p_str_title, x=0.05, y=0.95, xanchor='left', yanchor='top'), # Title inside top-left
        template="plotly_dark",
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False),
        plot_bgcolor="rgba(0,0,0,0)", # Transparent plot area
        paper_bgcolor="rgba(0,0,0,0)", # Transparent paper area
        margin=dict(l=40, r=20, t=60, b=40) # Adjust margins
    )
    return fig

# Define a consistent color map for seals (can be expanded)
# This should ideally be managed more dynamically if seals change often
# We will populate this within the callback based on the data received
dict_color_map = {}
list_plotly_colors = px.colors.qualitative.Plotly # Use a default sequence

def func_get_seal_color(p_int_seal_id):
    """Assigns a consistent color to a seal ID."""
    global dict_color_map, list_plotly_colors
    if p_int_seal_id not in dict_color_map:
        # Assign next color in sequence, wrap around if needed
        dict_color_map[p_int_seal_id] = list_plotly_colors[len(dict_color_map) % len(list_plotly_colors)]
    return dict_color_map[p_int_seal_id]


# --- App Layout ---
def func_serve_layout():
    """Creates the application layout dynamically."""
    # Load initial data for dropdowns
    l_list_dict_seals = data_query.func_get_unique_seal_id()
    l_list_seal_options = [{'label': str(row['sealId']), 'value': row['sealId']} for row in l_list_dict_seals]

    l_list_workload_options = [
        {'label': 'OLAP', 'value': 'olap'},
        {'label': 'CPOF-OLAP', 'value': 'cpof-olap'},
        {'label': 'OLTP', 'value': 'oltp'},
        {'label': 'CPOF-OLTP', 'value': 'cpof-oltp'},
        {'label': 'Mixed', 'value': 'mixed'},
        {'label': 'CPOF-Mixed', 'value': 'cpof-mixed'},
        {'label': 'Unknown', 'value': 'unknown'}
    ]

    l_dict_bookmarks = func_load_bookmarks()
    l_list_bookmark_options = func_get_bookmark_options(l_dict_bookmarks)


    return dbc.Container([
        # Store components for sharing state if needed (e.g., intermediate filtered data)
        # dcc.Store(id='store-intermediate-data'),
        dcc.Store(id='store-bookmark-data', data=l_dict_bookmarks), # Store loaded bookmarks
        dcc.Store(id='store-table-state'), # To help persist selection logic

        html.H1(APP_TITLE, style={'textAlign': 'center', 'marginBottom': '20px'}),

        # -- Top Row: Filters and Bookmarks --
        dbc.Row([
            dbc.Col(dcc.Dropdown(
                id='sealDropDown',
                options=l_list_seal_options,
                multi=True,
                placeholder="Select Application Seal ID(s)..."
            ), md=3),
            dbc.Col(dcc.Dropdown(
                id='workloadCategoyDropdown',
                options=l_list_workload_options,
                multi=True,
                placeholder="Select Workload Category(s)..."
                 # Note: workload filtering logic isn't directly tied to CLEAN_SPEND
                 # It will primarily be used for bookmarking the selection state
            ), md=3),
            dbc.Col(dcc.Dropdown(
                id='bookmarkDropdown',
                options=l_list_bookmark_options,
                placeholder="Load Bookmark...",
                clearable=True
            ), md=3),
            dbc.Col([
                dbc.Button("Create Bookmark", id="createBookmarkButton", n_clicks=0, size="sm", className="me-1"),
                dbc.Button("Delete Bookmark", id="deleteBookmarkButton", n_clicks=0, size="sm", color="danger")
            ], width="auto", className="d-flex align-items-center") # Use flex for alignment
        ], align="center", className="mb-4"), # Added margin bottom

         # Hidden input for bookmark name prompt
        html.Div(id='hidden-bookmark-input-div', style={'display': 'none'}, children=[
             dcc.Input(id='bookmark-name-input', type='text', placeholder='Enter bookmark name...'),
             html.Button('Save', id='save-bookmark-confirm-button', n_clicks=0),
             html.Button('Cancel', id='cancel-bookmark-button', n_clicks=0)
        ]),
        # Simple modal alternative using dbc.Modal for bookmark name
        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle("Create Bookmark")),
                dbc.ModalBody([
                    dbc.Label("Enter a name for this bookmark:"),
                    dbc.Input(id="bookmark-name-input-modal", placeholder="Bookmark Name", type="text"),
                ]),
                dbc.ModalFooter([
                    dbc.Button("Save", id="save-bookmark-confirm-button-modal", className="ms-auto", n_clicks=0),
                    dbc.Button("Cancel", id="cancel-bookmark-button-modal", color="secondary", n_clicks=0)
                ]),
            ],
            id="bookmark-modal",
            is_open=False,
        ),


        # -- Second Row: Charts and Table --
        dbc.Row([
            # Column 1: Charts
            dbc.Col([
                dbc.Card([
                     dbc.CardHeader("Total Spend Over Time"),
                     dbc.CardBody(dcc.Graph(id='totalSpendChart', figure=func_create_empty_figure()))
                ], className="mb-3"), # Added margin bottom
                dbc.Card([
                    dbc.CardHeader("Spend per GB Over Time"),
                    dbc.CardBody(dcc.Graph(id='spendPerGBChart', figure=func_create_empty_figure()))
                ])
            ], md=6),

            # Column 2: Table
            dbc.Col([
                 dbc.Card([
                     dbc.CardHeader("Spend Summary by Product"),
                     dbc.CardBody(
                        dash_table.DataTable(
                            id='ppgProductBySealTable',
                            columns=[
                                {"name": "Seal ID", "id": "sealId"},
                                {"name": "Product", "id": "ppgProduct"},
                                {"name": "Total Spend ($)", "id": "TotalSpend", "type": "numeric", "format": dash_table.Format.Format(precision=2, scheme=dash_table.Format.Scheme.fixed)},
                                {"name": "YoY Growth (%)", "id": "SpendGrowthOverYear", "type": "numeric", "format": dash_table.Format.Format(precision=1, scheme=dash_table.Format.Scheme.fixed).sign(dash_table.Format.Sign.positive)}
                            ],
                            data=[], # Initially empty
                            # Interactivity
                            filter_action="native", # Allow column filtering
                            sort_action="native",   # Allow column sorting
                            sort_mode="multi",      # Allow multi-column sorting
                            row_selectable="multi", # Allow selecting multiple rows
                            selected_rows=[],       # No rows selected initially
                            # Pagination
                            page_action="native",
                            page_current=0,
                            page_size=TABLE_PAGE_SIZE,
                            # Styling
                            style_table={'overflowX': 'auto', 'minWidth': '100%'}, # Ensure horizontal scroll if needed
                            style_cell={ # Apply basic styling consistent with dark theme
                                'backgroundColor': '#2a2a2a',
                                'color': '#d4d4d4',
                                'border': '1px solid #444444',
                                'padding': '8px',
                                'textAlign': 'left',
                                'whiteSpace': 'normal', # Allow text wrapping
                                'height': 'auto',
                                'minWidth': '80px', 'width': '150px', 'maxWidth': '250px' # Adjust width as needed
                            },
                             style_header={
                                'backgroundColor': '#333333',
                                'fontWeight': 'bold',
                                'color': '#e0e0e0',
                                'border': '1px solid #444444',
                            },
                             style_data_conditional=[ # Highlight selected rows
                                {
                                    'if': {'state': 'selected'},
                                    'backgroundColor': 'rgba(0, 123, 255, 0.3)', # Semi-transparent blue
                                    'border': '1px solid #007bff',
                                },
                            ],
                             style_filter={ # Style filter input boxes
                                'backgroundColor': '#333333',
                                'color': '#d4d4d4',
                                'border': '1px solid #555555',
                            },
                        )
                     )
                 ])
            ], md=6),
        ])

    ], fluid=True) # Use fluid container for full width

app.layout = func_serve_layout # Assign layout function

# --- Callbacks ---

# Callback to handle bookmark modal visibility
@callback(
    Output("bookmark-modal", "is_open"),
    Input("createBookmarkButton", "n_clicks"),
    Input("save-bookmark-confirm-button-modal", "n_clicks"),
    Input("cancel-bookmark-button-modal", "n_clicks"),
    State("bookmark-modal", "is_open"),
    prevent_initial_call=True,
)
def toggle_bookmark_modal(n_create, n_save, n_cancel, is_open):
    if ctx.triggered_id == "createBookmarkButton":
        return not is_open
    elif ctx.triggered_id in ["save-bookmark-confirm-button-modal", "cancel-bookmark-button-modal"]:
        return False
    return is_open

# Callback to handle bookmark creation/deletion/loading
@callback(
    Output("bookmarkDropdown", "options"),
    Output("bookmarkDropdown", "value"),
    Output("store-bookmark-data", "data"), # Update stored bookmarks
    # Outputs to reset filters when deleting or loading
    Output("sealDropDown", "value", allow_duplicate=True),
    Output("workloadCategoyDropdown", "value", allow_duplicate=True),
    Output("ppgProductBySealTable", "selected_rows", allow_duplicate=True),
    Input("save-bookmark-confirm-button-modal", "n_clicks"),
    Input("deleteBookmarkButton", "n_clicks"),
    Input("bookmarkDropdown", "value"), # Trigger loading when a bookmark is selected
    State("bookmark-name-input-modal", "value"),
    State("sealDropDown", "value"),
    State("workloadCategoyDropdown", "value"),
    State("ppgProductBySealTable", "selected_rows"),
    State("ppgProductBySealTable", "data"), # Need current table data to map selected rows
    State("store-bookmark-data", "data"), # Get current bookmarks
    prevent_initial_call=True # Important: prevent running on startup
)
def manage_bookmarks(
    save_clicks, delete_clicks, selected_bookmark,
    bookmark_name, selected_seals, selected_workloads,
    selected_table_rows_indices, table_data, current_bookmarks):

    trigger_id = ctx.triggered_id
    bookmarks = current_bookmarks if current_bookmarks else {}

    # Outputs defaults (no change unless specified)
    new_options = func_get_bookmark_options(bookmarks)
    new_bookmark_value = selected_bookmark
    new_store_data = bookmarks
    out_seals = dash.no_update
    out_workloads = dash.no_update
    out_selected_rows = dash.no_update


    if trigger_id == "save-bookmark-confirm-button-modal" and bookmark_name:
        print(f"Saving bookmark: {bookmark_name}")
        # Map selected row indices to unique identifiers (sealId, ppgProduct)
        selected_products_identifiers = []
        if selected_table_rows_indices and table_data:
            try:
                 selected_products_identifiers = [
                    (table_data[i]['sealId'], table_data[i]['ppgProduct'])
                    for i in selected_table_rows_indices if i < len(table_data) # Safety check
                 ]
            except IndexError:
                 print("Warning: Index error mapping selected rows during bookmark save.")
            except KeyError:
                 print("Warning: Key error ('sealId' or 'ppgProduct') mapping selected rows.")


        bookmarks[bookmark_name] = {
            "sealIds": selected_seals if selected_seals else [],
            "workloadCategories": selected_workloads if selected_workloads else [],
            "selectedProducts": selected_products_identifiers # Store identifiers
        }
        func_save_bookmarks(bookmarks)
        new_options = func_get_bookmark_options(bookmarks)
        new_store_data = bookmarks
        new_bookmark_value = bookmark_name # Select the newly created bookmark
        print("Bookmark saved.")

    elif trigger_id == "deleteBookmarkButton" and selected_bookmark:
        print(f"Deleting bookmark: {selected_bookmark}")
        if selected_bookmark in bookmarks:
            del bookmarks[selected_bookmark]
            func_save_bookmarks(bookmarks)
            new_options = func_get_bookmark_options(bookmarks)
            new_store_data = bookmarks
            new_bookmark_value = None # Clear selection
            # Reset filters
            out_seals = []
            out_workloads = []
            out_selected_rows = []
            print("Bookmark deleted and filters reset.")
        else:
             print("Bookmark not found for deletion.")

    elif trigger_id == "bookmarkDropdown" and selected_bookmark:
        print(f"Loading bookmark: {selected_bookmark}")
        if selected_bookmark in bookmarks:
            bookmark_data = bookmarks[selected_bookmark]
            out_seals = bookmark_data.get("sealIds", [])
            out_workloads = bookmark_data.get("workloadCategories", [])
            # We will handle applying the selected_products filter in the main update callback
            # Store the target product identifiers for the main callback to use
            # Using a temporary store or passing via Output could work, but let's
            # rely on the main callback reading the bookmark state.
            # Resetting selected rows here might be premature before table data updates.
            # Let the main callback handle selection based on loaded bookmark state.
            out_selected_rows = [] # Clear selection initially, main CB will re-apply
            print(f"Bookmark loaded. Filters set for Seals: {out_seals}, Workloads: {out_workloads}")
            print(f"Target products: {bookmark_data.get('selectedProducts', [])}")

        else:
             print("Selected bookmark not found in stored data.")


    # Ensure prevent_initial_call works correctly by providing default outputs
    # Need to use allow_duplicate=True on the filter outputs
    # because they are also targeted by the main callback.

    return new_options, new_bookmark_value, new_store_data, out_seals, out_workloads, out_selected_rows


# Main callback to update table and charts based on filters and table selections
@callback(
    Output("ppgProductBySealTable", "data"),
    Output("ppgProductBySealTable", "selected_rows", allow_duplicate=True), # Update selection based on filters/bookmarks
    Output("totalSpendChart", "figure"),
    Output("spendPerGBChart", "figure"),
    Output("store-table-state", "data"), # Store current table data+selection state
    Input("sealDropDown", "value"),
    Input("workloadCategoyDropdown", "value"), # Although not filtering data, include for bookmarking consistency
    Input("ppgProductBySealTable", "selected_rows"),
    # Trigger when a bookmark is loaded (via the bookmarkDropdown value changing)
    # or when filters are reset by bookmark deletion (also changes bookmarkDropdown value)
    Input("bookmarkDropdown", "value"),
    State("ppgProductBySealTable", "data"), # Get previous data to maintain selection if possible
    State("store-bookmark-data", "data"), # Get bookmark definitions
    State("store-table-state", "data"), # Get previous state if needed
    prevent_initial_call=True
)
def update_dashboard(
    selected_seals, selected_workloads,
    selected_table_rows_indices,
    selected_bookmark, # The *name* of the currently loaded bookmark (or None)
    previous_table_data, # Data currently in the table
    bookmarks, # All loaded bookmark definitions
    previous_table_state): # Previous data and selection state


    print("-" * 20)
    print(f"Callback Triggered by: {ctx.triggered_id}")
    print(f"Selected Seals: {selected_seals}")
    print(f"Selected Workloads: {selected_workloads}") # Info purpose
    print(f"Selected Table Rows Indices: {selected_table_rows_indices}")
    print(f"Selected Bookmark Name: {selected_bookmark}")


    # --- 1. Determine Filters ---
    # Seal filter is the primary data filter here
    list_filter_seals = selected_seals if selected_seals else [] # Empty list means no filter applied in query

    # --- 2. Fetch Data for Table ---
    list_dict_table_data_full = data_query.func_get_ppg_product_summary(p_list_seal_ids=list_filter_seals)

    # --- 3. Determine Row Selection Logic ---
    list_current_selected_rows = selected_table_rows_indices if selected_table_rows_indices is not None else []
    list_target_selected_products = [] # (sealId, ppgProduct) tuples

    # If a bookmark was just loaded, its product selections take precedence
    # Check if the trigger was the bookmark dropdown AND a bookmark is selected
    # This handles both loading a new bookmark and the state after deleting one (value becomes None)
    bookmark_triggered_load = ctx.triggered_id == "bookmarkDropdown" and selected_bookmark is not None

    if bookmark_triggered_load:
        if bookmarks and selected_bookmark in bookmarks:
            list_target_selected_products = [tuple(prod) for prod in bookmarks[selected_bookmark].get("selectedProducts", [])]
            print(f"Applying bookmark product selection: {list_target_selected_products}")
        list_current_selected_rows = [] # Reset indices, we'll calculate new ones

    # If not loading a bookmark, try to preserve selection from the previous state
    elif previous_table_data and list_current_selected_rows and ctx.triggered_id != "bookmarkDropdown":
        try:
            list_target_selected_products = [
                (previous_table_data[i]['sealId'], previous_table_data[i]['ppgProduct'])
                for i in list_current_selected_rows if i < len(previous_table_data)
            ]
            print(f"Preserving previous selection: {list_target_selected_products}")
        except (IndexError, KeyError):
             print("Warning: Could not map previous selected rows to identifiers.")
             list_target_selected_products = []


    # --- 4. Calculate New Selected Row Indices ---
    list_new_selected_row_indices = []
    if list_target_selected_products:
        # Create a lookup for faster checking
        set_target_products = set(list_target_selected_products)
        list_new_selected_row_indices = [
            idx for idx, row in enumerate(list_dict_table_data_full)
            if (row['sealId'], row['ppgProduct']) in set_target_products
        ]
        print(f"Calculated new selected row indices: {list_new_selected_row_indices}")


    # --- 5. Determine Filters for Charts ---
    list_chart_filter_seals = list_filter_seals
    list_chart_filter_products = None

    # If rows ARE selected in the table (either preserved or from bookmark), filter charts by those products
    if list_new_selected_row_indices: # Use the newly calculated indices
        set_selected_product_names = set(
            list_dict_table_data_full[i]['ppgProduct']
            for i in list_new_selected_row_indices if i < len(list_dict_table_data_full)
        )
        # We also need to ensure we only get data for the seals involved in the selection
        set_selected_seal_ids = set(
             list_dict_table_data_full[i]['sealId']
            for i in list_new_selected_row_indices if i < len(list_dict_table_data_full)
        )
        list_chart_filter_products = list(set_selected_product_names)
        # Override the main seal filter if table selection narrows it down
        list_chart_filter_seals = list(set_selected_seal_ids)
        print(f"Filtering charts by selected products: {list_chart_filter_products} for Seals: {list_chart_filter_seals}")


    # --- 6. Fetch Data for Charts ---
    list_dict_total_spend = data_query.func_get_total_spend_over_time(
        p_list_seal_ids=list_chart_filter_seals,
        p_list_ppg_products=list_chart_filter_products
    )
    list_dict_spend_per_gb = data_query.func_get_spend_per_gb_over_time(
        p_list_seal_ids=list_chart_filter_seals,
        p_list_ppg_products=list_chart_filter_products
    )

    # Convert to DataFrames for easier plotting (alternatively, loop through dicts)
    df_total_spend = pd.DataFrame(list_dict_total_spend)
    df_spend_per_gb = pd.DataFrame(list_dict_spend_per_gb)
    # Ensure date columns are datetime objects for Plotly
    if not df_total_spend.empty:
        df_total_spend['date'] = pd.to_datetime(df_total_spend['date'])
    if not df_spend_per_gb.empty:
        df_spend_per_gb['date'] = pd.to_datetime(df_spend_per_gb['date'])

    # --- 7. Create Chart Figures ---

    # Total Spend Chart
    if not df_total_spend.empty:
         # Assign consistent colors
        color_discrete_map_spend = {seal: func_get_seal_color(seal) for seal in df_total_spend['sealId'].unique()}

        fig_total_spend = px.line(
            df_total_spend,
            x='date',
            y='TotalSpend',
            color='sealId',
            title=None, # Remove default title, using card header
            labels={'TotalSpend': 'Total Spend ($)', 'date': 'Date', 'sealId': 'Seal ID'},
            template='plotly_dark',
            markers=True, # Show markers on lines
            color_discrete_map=color_discrete_map_spend
        )
        fig_total_spend.update_layout(
            title=dict(text=None), # Ensure no plotly title
            legend_title_text='Seal ID',
            hovermode='x unified', # Show hover info for all lines at a given x
            margin=dict(l=40, r=20, t=10, b=40) # Reduced top margin
        )
    else:
        fig_total_spend = func_create_empty_figure() # Use placeholder if no data


    # Spend per GB Chart
    if not df_spend_per_gb.empty:
        # Assign consistent colors
        color_discrete_map_gb = {seal: func_get_seal_color(seal) for seal in df_spend_per_gb['sealId'].unique()}

        fig_spend_per_gb = px.line(
            df_spend_per_gb,
            x='date',
            y='SpendPerGB',
            color='sealId',
            title=None,
            labels={'SpendPerGB': 'Spend per GB ($/GB)', 'date': 'Date', 'sealId': 'Seal ID'},
            template='plotly_dark',
            markers=True,
            color_discrete_map=color_discrete_map_gb
        )
        fig_spend_per_gb.update_layout(
            title=dict(text=None),
            legend_title_text='Seal ID',
            hovermode='x unified',
             margin=dict(l=40, r=20, t=10, b=40)
        )
    else:
        fig_spend_per_gb = func_create_empty_figure()


    # --- 8. Store state for next callback ---
    # Storing the current data and selection might help preserve state,
    # but mapping identifiers as done above is generally more robust.
    dict_current_table_state = {
        "data": list_dict_table_data_full,
        "selected_rows": list_new_selected_row_indices
    }

    print(f"Returning {len(list_dict_table_data_full)} rows for table.")
    print(f"Returning selected indices: {list_new_selected_row_indices}")
    print("-" * 20)

    return (
        list_dict_table_data_full,
        list_new_selected_row_indices,
        fig_total_spend,
        fig_spend_per_gb,
        dict_current_table_state
    )


# --- Run the App ---
if __name__ == '__main__':
    # Ensure sample data exists
    if not os.path.exists(data_query.DB_FILE):
        print(f"Database file {data_query.DB_FILE} not found. Running schema setup...")
        # Need to be able to import and run the setup function
        from data import schemaSetup
        schemaSetup.func_populate_database()
        print("Database setup complete.")
    else:
        print(f"Using existing database file: {data_query.DB_FILE}")

    # Run the Dash app
    app.run_server(debug=True) # Turn off debug for production