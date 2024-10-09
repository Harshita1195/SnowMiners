from typing import Optional

import streamlit as st
from streamlit_extras.row import row

from semantic_model_generator.protos import semantic_model_pb2

SUPPORTED_JOIN_TYPES = [
    join_type
    for join_type in semantic_model_pb2.JoinType.values()
    if join_type != semantic_model_pb2.JoinType.join_type_unknown
]
SUPPORTED_RELATIONSHIP_TYPES = [
    relationship_type
    for relationship_type in semantic_model_pb2.RelationshipType.values()
    if relationship_type
    != semantic_model_pb2.RelationshipType.relationship_type_unknown
]


def relationship_builder(
    relationship: semantic_model_pb2.Relationship, key: Optional[int] = 0
) -> None:
    """
    Renders a UI for building/editing a semantic model relationship.
    Args:
        relationship: The relationship object to edit.

    Returns:

    """
    with st.expander(
        relationship.name or f"{relationship.left_table} ↔️ {relationship.right_table}",
        expanded=True,
    ):
        relationship.name = st.text_input(
            "Name", value=relationship.name, key=f"name_{key}"
        )
        # Logic to preselect the tables in the dropdown based on what's in the semantic model.
        try:
            default_left_table = [
                table.name for table in st.session_state.semantic_model.tables
            ].index(relationship.left_table)
            default_right_table = [
                table.name for table in st.session_state.semantic_model.tables
            ].index(relationship.right_table)
        except ValueError:
            default_left_table = 0
            default_right_table = 0
        relationship.left_table = st.selectbox(
            "Left Table",
            options=[table.name for table in st.session_state.semantic_model.tables],
            index=default_left_table,
            key=f"left_table_{key}",
        )

        relationship.right_table = st.selectbox(
            "Right Table",
            options=[table.name for table in st.session_state.semantic_model.tables],
            index=default_right_table,
            key=f"right_table_{key}",
        )

        relationship.join_type = st.radio(  # type: ignore
            "Join Type",
            options=SUPPORTED_JOIN_TYPES,
            format_func=lambda join_type: semantic_model_pb2.JoinType.Name(join_type),
            index=SUPPORTED_JOIN_TYPES.index(relationship.join_type),
            key=f"join_type_{key}",
        )

        relationship.relationship_type = st.radio(  # type: ignore
            "Relationship Type",
            options=SUPPORTED_RELATIONSHIP_TYPES,
            format_func=lambda relationship_type: semantic_model_pb2.RelationshipType.Name(
                relationship_type
            ),
            index=SUPPORTED_RELATIONSHIP_TYPES.index(relationship.relationship_type),
            key=f"relationship_type_{key}",
        )

        st.divider()
        # Builder section for the relationship's columns.
        for col_idx, join_cols in enumerate(relationship.relationship_columns):
            # Grabbing references to the exact Table objects that the relationship is pointing to.
            # This allows us to pull the columns.
            left_table_object = next(
                (
                    table
                    for table in st.session_state.semantic_model.tables
                    if table.name == relationship.left_table
                )
            )
            right_table_object = next(
                (
                    table
                    for table in st.session_state.semantic_model.tables
                    if table.name == relationship.right_table
                )
            )

            try:
                left_columns = []
                left_columns.extend(left_table_object.columns)
                left_columns.extend(left_table_object.dimensions)
                left_columns.extend(left_table_object.time_dimensions)
                left_columns.extend(left_table_object.measures)

                right_columns = []
                right_columns.extend(right_table_object.columns)
                right_columns.extend(right_table_object.dimensions)
                right_columns.extend(right_table_object.time_dimensions)
                right_columns.extend(right_table_object.measures)

                default_left_col = [col.name for col in left_columns].index(
                    join_cols.left_column
                )
                default_right_col = [col.name for col in right_columns].index(
                    join_cols.right_column
                )
            except ValueError:
                default_left_col = 0
                default_right_col = 0

            join_cols.left_column = st.selectbox(
                "Left Column",
                options=[col.name for col in left_columns],
                index=default_left_col,
                key=f"left_col_{key}_{col_idx}",
            )
            join_cols.right_column = st.selectbox(
                "Right Column",
                options=[col.name for col in right_columns],
                index=default_right_col,
                key=f"right_col_{key}_{col_idx}",
            )

            if st.button("Delete join key", key=f"delete_join_key_{key}_{col_idx}"):
                relationship.relationship_columns.pop(col_idx)
                st.rerun(scope="fragment")

            st.divider()

        join_editor_row = row(2, vertical_align="center")
        if join_editor_row.button(
            "Add new join key",
            key=f"add_join_keys_{key}",
            use_container_width=True,
            type="primary",
        ):
            relationship.relationship_columns.append(
                semantic_model_pb2.RelationKey(
                    left_column="",
                    right_column="",
                )
            )
            st.rerun(scope="fragment")

        if join_editor_row.button(
            "🗑️ Delete join path",
            key=f"delete_join_path_{key}",
            use_container_width=True,
        ):
            st.session_state.builder_joins.pop(key)
            st.rerun(scope="fragment")


@st.dialog("Join Builder", width="large")
def joins_dialog() -> None:

    if "builder_joins" not in st.session_state:
        # Making a copy of the original relationships list so we can modify freely without affecting the original.
        st.session_state.builder_joins = st.session_state.semantic_model.relationships[
            :
        ]

    for idx, relationship in enumerate(st.session_state.builder_joins):
        relationship_builder(relationship, idx)

    # If the user clicks "Add join", add a new join to the relationships list
    if st.button("Add new join path", use_container_width=True):
        st.session_state.builder_joins.append(
            semantic_model_pb2.Relationship(
                left_table="",
                right_table="",
                join_type=semantic_model_pb2.JoinType.inner,
                relationship_type=semantic_model_pb2.RelationshipType.one_to_one,
                relationship_columns=[],
            )
        )
        st.rerun(scope="fragment")

    # If the user clicks "Save", save the relationships list to the session state
    if st.button("Save to semantic model", use_container_width=True, type="primary"):
        # Quickly validate that all of the user's joins have the required fields.
        for relationship in st.session_state.builder_joins:
            if not relationship.left_table or not relationship.right_table:
                st.error("Please fill out left and right tables for all join paths.")
                return

            if not relationship.name:
                st.error(
                    f"The join path between {relationship.left_table} and {relationship.right_table} is missing a name."
                )
                return

            if not relationship.relationship_columns:
                st.error(
                    f"The join path between {relationship.left_table} and {relationship.right_table} is missing joinable columns."
                )
                return

        del st.session_state.semantic_model.relationships[:]
        st.session_state.semantic_model.relationships.extend(
            st.session_state.builder_joins
        )
        st.session_state.validated = None
        st.rerun()
