import csv
import json
import logging
import tempfile
import os
from pydantic import BaseModel
from typing import IO, Any, Optional, List
from uuid import UUID, uuid4

from asyncpg.exceptions import UniqueViolationError
from fastapi import HTTPException

from core.base import (
    DatabaseConfig,
    Handler,
    KGExtractionStatus,
    R2RException,
    generate_default_user_collection_id,
)
from core.base.abstractions import (
    DocumentResponse,
    DocumentType,
    IngestionStatus,
)
from core.base.api.models import CollectionResponse
from core.utils import generate_default_user_collection_id

from .base import PostgresConnectionManager

logger = logging.getLogger()

# New field for subcollections


class PostgresCollectionsHandler(Handler):
    TABLE_NAME = "collections"

    def __init__(
        self,
        project_name: str,
        connection_manager: PostgresConnectionManager,
        config: DatabaseConfig,
    ):
        self.config = config
        super().__init__(project_name, connection_manager)

    async def create_tables(self) -> None:
        # First create the base table
        query = f"""
        CREATE TABLE IF NOT EXISTS {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)} (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            owner_id UUID,
            name TEXT NOT NULL,
            description TEXT,
            theme TEXT DEFAULT '#a855f7',
            icon TEXT DEFAULT 'Book',
            graph_sync_status TEXT DEFAULT 'pending',
            graph_cluster_status TEXT DEFAULT 'pending',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            user_count INT DEFAULT 0,
            document_count INT DEFAULT 0,
            parent_id UUID REFERENCES {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}(id) ON DELETE SET NULL,
            subcollections UUID[] DEFAULT ARRAY[]::UUID[] 
        );

        -- Add theme column if it doesn't exist
        DO $$ 
        BEGIN 
            IF NOT EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_name = '{self.TABLE_NAME}' 
                AND column_name = 'theme'
                AND table_schema = '{self.project_name}'
            ) THEN
                ALTER TABLE {self._get_table_name(self.TABLE_NAME)} 
                ADD COLUMN theme TEXT DEFAULT '#a855f7';
            END IF;
        END $$;

        -- Add icon column if it doesn't exist
        DO $$ 
        BEGIN 
            IF NOT EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_name = '{self.TABLE_NAME}' 
                AND column_name = 'icon'
                AND table_schema = '{self.project_name}'
            ) THEN
                ALTER TABLE {self._get_table_name(self.TABLE_NAME)} 
                ADD COLUMN icon TEXT DEFAULT 'Book';
            END IF;
        END $$;
        """
        await self.connection_manager.execute_query(query)
    # Add subcollections column if it doesn't exist
        

    async def collection_exists(self, collection_id: UUID) -> bool:
        """Check if a collection exists, either as a root collection or as a subcollection."""
        query = f"""
            WITH RECURSIVE collection_tree AS (
                -- Base case: direct collection
                SELECT id FROM {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
                WHERE id = $1
                
                UNION
                
                -- Recursive case: check subcollections
                SELECT unnest(c.subcollections)
                FROM {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)} c
                WHERE c.subcollections IS NOT NULL
            )
            SELECT EXISTS (
                SELECT 1 FROM collection_tree
            );
        """
        result = await self.connection_manager.fetchrow_query(query, [collection_id])
        return result["exists"] if result else False

    async def create_collection(
        self,
        owner_id: UUID,
        name: Optional[str] = None,
        description: str = "",
        collection_id: Optional[UUID] = None,
        parent_id: Optional[UUID] = None,
        theme: Optional[str] = None,
        icon: Optional[str] = None,
    ) -> CollectionResponse:
        if not name and not collection_id:
            name = self.config.default_collection_name
            collection_id = generate_default_user_collection_id(owner_id)

        # Get defaults from environment variables or use hardcoded defaults
        default_theme = os.getenv("R2R_DEFAULT_COLLECTION_THEME", "#a855f7")
        default_icon = os.getenv("R2R_DEFAULT_COLLECTION_ICON", "Book")
        main_subcollection_name = os.getenv("R2R_MAIN_SUBCOLLECTION_NAME", "Main")
        main_subcollection_desc = os.getenv("R2R_MAIN_SUBCOLLECTION_DESC", "Main subcollection")
        main_subcollection_theme = os.getenv("R2R_MAIN_SUBCOLLECTION_THEME", default_theme)
        main_subcollection_icon = os.getenv("R2R_MAIN_SUBCOLLECTION_ICON", default_icon)

        # Default subcollections configuration from env
        default_subs = [
            (
                os.getenv("R2R_TEXTBOOKS_NAME", "Textbooks"),
                os.getenv("R2R_TEXTBOOKS_DESC", "General documents collection"),
                os.getenv("R2R_TEXTBOOKS_THEME", default_theme),
                os.getenv("R2R_TEXTBOOKS_ICON", "BookOpen"),
            ),
            (
                os.getenv("R2R_ASSIGNMENTS_NAME", "Assignments"),
                os.getenv("R2R_ASSIGNMENTS_DESC", "Assignment instructions and solutions"),
                os.getenv("R2R_ASSIGNMENTS_THEME", default_theme),
                os.getenv("R2R_ASSIGNMENTS_ICON", "ClipboardList"),
            ),
            (
                os.getenv("R2R_NOTES_NAME", "Notes"),
                os.getenv("R2R_NOTES_DESC", "Class notes eg. written notes"),
                os.getenv("R2R_NOTES_THEME", default_theme),
                os.getenv("R2R_NOTES_ICON", "PencilSquare"),
            ),
        ]

        # Validate parent_id if provided
        if parent_id:
            if not await self.collection_exists(parent_id):
                raise R2RException(
                    status_code=404,
                    message="Parent collection not found",
                )
            parent_collection = await self.get_collection_by_id(parent_id)
            if parent_collection.owner_id != owner_id:
                raise R2RException(
                    status_code=403,
                    message="You do not have permission to create a subcollection in this parent collection",
                )

        # Create the collection
        query = f"""
            INSERT INTO {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
            (id, owner_id, name, description, parent_id, theme, icon)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id, owner_id, name, description, graph_sync_status, 
                      graph_cluster_status, created_at, updated_at, parent_id,
                      subcollections, theme, icon
        """
        params = [
            collection_id or uuid4(),
            owner_id,
            name,
            description,
            parent_id,
            theme or default_theme,
            icon or default_icon,
        ]

        try:
            result = await self.connection_manager.fetchrow_query(
                query=query,
                params=params,
            )

            # Update parent's subcollections if this is a subcollection
            if parent_id:
                update_parent_query = f"""
                    UPDATE {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
                    SET subcollections = array_append(
                        COALESCE(subcollections, ARRAY[]::UUID[]), 
                        $1
                    )
                    WHERE id = $2
                """
                await self.connection_manager.execute_query(
                    update_parent_query, [result["id"], parent_id]
                )

                return await self.get_collection_by_id(
                    result["id"], include_children=True
                )
            else:
                # If no parent, create default subcollections
                main_sub_id = uuid4()
                
                # Create main subcollection
                await self.connection_manager.fetchrow_query(
                    query,
                    [
                        main_sub_id,
                        owner_id,
                        main_subcollection_name,
                        main_subcollection_desc,
                        result["id"],
                        main_subcollection_theme,
                        main_subcollection_icon,
                    ],
                )

                # Update root collection's subcollections array with main subcollection
                update_root_query = f"""
                    UPDATE {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
                    SET subcollections = array_append(
                        COALESCE(subcollections, ARRAY[]::UUID[]), 
                        $1
                    )
                    WHERE id = $2
                """
                await self.connection_manager.execute_query(
                    update_root_query, [main_sub_id, result["id"]]
                )

                # Create default subcollections under main
                for sub_name, sub_desc, sub_theme, sub_icon in default_subs:
                    sub_result = await self.connection_manager.fetchrow_query(
                        query,
                        [
                            uuid4(),
                            owner_id,
                            sub_name,
                            sub_desc,
                            main_sub_id,
                            sub_theme,
                            sub_icon,
                        ],
                    )
                    # Update main subcollection's subcollections array
                    await self.connection_manager.execute_query(
                        update_root_query, [sub_result["id"], main_sub_id]
                    )

                # Get the full collection with subcollections
                return await self.get_collection_by_id(result["id"], include_children=True)

        except UniqueViolationError:
            raise R2RException(
                message="Collection with this ID already exists",
                status_code=409,
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"An error occurred while creating the collection: {e}",
            ) from e

   
    async def get_collection_by_id(
        self, collection_id: UUID, include_children: bool = False
    ) -> CollectionResponse:
        query = f"""
            SELECT 
                id,
                owner_id,
                name,
                description,
                theme,
                icon,
                graph_sync_status,
                graph_cluster_status,
                created_at,
                updated_at,
                user_count,
                document_count,
                parent_id,
                COALESCE(subcollections, ARRAY[]::UUID[]) AS subcollections
            FROM {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
            WHERE id = $1
        """
        
        result = await self.connection_manager.fetchrow_query(query, [collection_id])

        if not result:
            raise R2RException(status_code=404, message="Collection not found")

        collection = CollectionResponse(
            id=result["id"],
            owner_id=result["owner_id"],
            name=result["name"],
            description=result["description"],
            theme=result["theme"],
            icon=result["icon"],
            graph_cluster_status=result["graph_cluster_status"],
            graph_sync_status=result["graph_sync_status"],
            created_at=result["created_at"],
            updated_at=result["updated_at"],
            user_count=result.get("user_count", 0),
            document_count=result.get("document_count", 0),
            parent_id=result["parent_id"],
            subcollections=result["subcollections"]
        )

        if include_children and result["subcollections"]:
            # Recursively fetch subcollections
            subcollections = []
            for sub_id in result["subcollections"]:
                try:
                    sub_collection = await self.get_collection_by_id(sub_id, include_children=True)
                    subcollections.append(sub_collection)
                except R2RException:
                    # If subcollection not found, skip it
                    continue
            collection.subcollection_details = subcollections

        return collection

    async def update_collection(
        self,
        collection_id: UUID,
        name: Optional[str] = None,
        description: Optional[str] = None,
        parent_id: Optional[UUID] = None,
    ) -> CollectionResponse:
        """Update an existing collection."""
        if not await self.collection_exists(collection_id):
            raise R2RException(status_code=404, message="Collection not found")

        # If parent_id is provided, verify it exists
        if parent_id is not None and not await self.collection_exists(parent_id):
            raise R2RException(status_code=404, message="Parent collection not found")

        update_fields = []
        params: list = []
        param_index = 1

        if name is not None:
            update_fields.append(f"name = ${param_index}")
            params.append(name)
            param_index += 1

        if description is not None:
            update_fields.append(f"description = ${param_index}")
            params.append(description)
            param_index += 1

        if parent_id is not None:
            # First, remove this collection from its current parent's subcollections array
            old_parent_query = f"""
                UPDATE {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
                SET subcollections = array_remove(subcollections, $1)
                WHERE id IN (
                    SELECT parent_id 
                    FROM {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
                    WHERE id = $1
                )
            """
            await self.connection_manager.execute_query(old_parent_query, [collection_id])

            # Then update the parent_id and add to new parent's subcollections
            update_fields.append(f"parent_id = ${param_index}")
            params.append(parent_id)
            param_index += 1

            # Add this collection to the new parent's subcollections array
            new_parent_query = f"""
                UPDATE {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
                SET subcollections = array_append(subcollections, $1)
                WHERE id = $2
            """
            await self.connection_manager.execute_query(new_parent_query, [collection_id, parent_id])

        if not update_fields:
            raise R2RException(status_code=400, message="No fields to update")

        update_fields.append("updated_at = NOW()")
        params.append(collection_id)

        query = f"""
            WITH updated_collection AS (
                UPDATE {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
                SET {", ".join(update_fields)}
                WHERE id = ${param_index}
                RETURNING id, owner_id, name, description, graph_sync_status, graph_cluster_status, created_at, updated_at, parent_id, subcollections
            )
            SELECT
                uc.*,
                COUNT(DISTINCT u.id) FILTER (WHERE u.id IS NOT NULL) as user_count,
                COUNT(DISTINCT d.id) FILTER (WHERE d.id IS NOT NULL) as document_count
            FROM updated_collection uc
            LEFT JOIN {self._get_table_name("users")} u ON uc.id = ANY(u.collection_ids)
            LEFT JOIN {self._get_table_name("documents")} d ON uc.id = ANY(d.collection_ids)
            GROUP BY uc.id, uc.owner_id, uc.name, uc.description, uc.graph_sync_status, uc.graph_cluster_status, uc.created_at, uc.updated_at, uc.parent_id, uc.subcollections
        """
        try:
            result = await self.connection_manager.fetchrow_query(query, params)
            if not result:
                raise R2RException(status_code=404, message="Collection not found")

            return CollectionResponse(
                id=result["id"],
                owner_id=result["owner_id"],
                name=result["name"],
                description=result["description"],
                graph_sync_status=result["graph_sync_status"],
                graph_cluster_status=result["graph_cluster_status"],
                created_at=result["created_at"],
                updated_at=result["updated_at"],
                user_count=result["user_count"],
                document_count=result["document_count"],
                parent_id=result["parent_id"],
                subcollections=result["subcollections"] or [],
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"An error occurred while updating the collection: {e}",
            ) from e

    async def delete_collection_relational(self, collection_id: UUID) -> None:
        # Remove collection_id from users
        user_update_query = f"""
            UPDATE {self._get_table_name("users")}
            SET collection_ids = array_remove(collection_ids, $1)
            WHERE $1 = ANY(collection_ids)
        """
        await self.connection_manager.execute_query(user_update_query, [collection_id])

        # Remove collection_id from documents
        document_update_query = f"""
            WITH updated AS (
                UPDATE {self._get_table_name("documents")}
                SET collection_ids = array_remove(collection_ids, $1)
                WHERE $1 = ANY(collection_ids)
                RETURNING 1
            )
            SELECT COUNT(*) AS affected_rows FROM updated
        """
        await self.connection_manager.fetchrow_query(
            document_update_query, [collection_id]
        )

        # Delete the collection
        delete_query = f"""
            DELETE FROM {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
            WHERE id = $1
            RETURNING id
        """
        deleted = await self.connection_manager.fetchrow_query(
            delete_query, [collection_id]
        )

        if not deleted:
            raise R2RException(status_code=404, message="Collection not found")

    async def documents_in_collection(
        self, collection_id: UUID, offset: int, limit: int
    ) -> dict[str, list[DocumentResponse] | int]:
        """
        Get all documents in a specific collection with pagination.
        Args:
            collection_id (UUID): The ID of the collection to get documents from.
            offset (int): The number of documents to skip.
            limit (int): The maximum number of documents to return.
        Returns:
            List[DocumentResponse]: A list of DocumentResponse objects representing the documents in the collection.
        Raises:
            R2RException: If the collection doesn't exist.
        """
        if not await self.collection_exists(collection_id):
            raise R2RException(status_code=404, message="Collection not found")
        
        query = f"""
            SELECT d.id, d.owner_id, d.type, d.metadata, d.title, d.version,
                d.size_in_bytes, d.ingestion_status, d.extraction_status, d.created_at, d.updated_at, d.summary,
                COUNT(*) OVER() AS total_entries
            FROM {self._get_table_name("documents")} d
            WHERE $1 = ANY(d.collection_ids)
            ORDER BY d.created_at DESC
            OFFSET $2
        """

        conditions = [collection_id, offset]
        if limit != -1:
            query += " LIMIT $3"
            conditions.append(limit)

        results = await self.connection_manager.fetch_query(query, conditions)
        documents = [
            DocumentResponse(
                id=row["id"],
                collection_ids=[collection_id],  # Only return the requested collection ID
                owner_id=row["owner_id"],
                document_type=DocumentType(row["type"]),
                metadata=json.loads(row["metadata"]),
                title=row["title"],
                version=row["version"],
                size_in_bytes=row["size_in_bytes"],
                ingestion_status=IngestionStatus(row["ingestion_status"]),
                extraction_status=KGExtractionStatus(row["extraction_status"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                summary=row["summary"],
            )
            for row in results
        ]
        total_entries = results[0]["total_entries"] if results else 0

        return {"results": documents, "total_entries": total_entries}

    async def get_collections_overview(
        self,
        offset: int = 0,
        limit: int = 100,
        filter_user_ids: Optional[list[UUID]] = None,
        filter_document_ids: Optional[list[UUID]] = None,
        filter_collection_ids: Optional[list[UUID]] = None,
    ) -> dict[str, list[CollectionResponse] | int]:
        try:
            conditions = []
            params: list[Any] = []
            param_index = 1

            if filter_user_ids:
                conditions.append(
                    f"""
                    c.id IN (
                        SELECT unnest(collection_ids)
                        FROM {self.project_name}.users
                        WHERE id = ANY(${param_index})
                    )
                    """
                )
                params.append(filter_user_ids)
                param_index += 1

            if filter_document_ids:
                conditions.append(
                    f"""
                    c.id IN (
                        SELECT unnest(collection_ids)
                        FROM {self.project_name}.documents
                        WHERE id = ANY(${param_index})
                    )
                    """
                )
                params.append(filter_document_ids)
                param_index += 1

            if filter_collection_ids:
                conditions.append(f"c.id = ANY(${param_index})")
                params.append(filter_collection_ids)
                param_index += 1

            # Only get root collections (no parent)
            conditions.append("c.parent_id IS NULL")

            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            # First, get root collections with pagination
            root_query = f"""
                SELECT 
                    c.*,
                    COUNT(*) OVER() as total_entries
                FROM {self.project_name}.collections c
                {where_clause}
                ORDER BY c.created_at DESC
                OFFSET ${param_index}
                LIMIT ${param_index + 1}
            """
            params.extend([offset, limit])

            root_results = await self.connection_manager.fetch_query(root_query, params)

            if not root_results:
                return {"results": [], "total_entries": 0}

            total_entries = root_results[0]["total_entries"] if root_results else 0

            # Build collection hierarchy
            collections = []
            for row in root_results:
                collection = CollectionResponse(
                    id=row["id"],
                    owner_id=row["owner_id"],
                    name=row["name"],
                    description=row["description"],
                    theme=row.get("theme", "#a855f7"),
                    icon=row.get("icon", "Book"),
                    graph_cluster_status=row["graph_cluster_status"],
                    graph_sync_status=row["graph_sync_status"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                    user_count=row.get("user_count", 0),
                    document_count=row.get("document_count", 0),
                    parent_id=None,  # These are root collections
                    subcollections=row["subcollections"] or [],
                )

                # Fetch all subcollections recursively
                if row["subcollections"]:
                    subcollections = []
                    for sub_id in row["subcollections"]:
                        try:
                            sub_collection = await self.get_collection_by_id(sub_id, include_children=True)
                            subcollections.append(sub_collection)
                        except R2RException:
                            continue
                    collection.subcollection_details = subcollections

                collections.append(collection)

            return {"results": collections, "total_entries": total_entries}
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"An error occurred while fetching collections: {str(e)}",
            ) from e

    async def assign_document_to_collection_relational(
        self,
        document_id: UUID,
        collection_id: UUID,
    ) -> UUID:
        """
        Assign a document to a collection and update document counts for the collection
        and all its parent collections.
        """
        try:
            if not await self.collection_exists(collection_id):
                raise R2RException(status_code=404, message="Collection not found")

            # First, check if the document exists
            document_check_query = f"""
                SELECT 1 FROM {self._get_table_name("documents")}
                WHERE id = $1
            """
            document_exists = await self.connection_manager.fetchrow_query(
                document_check_query, [document_id]
            )

            if not document_exists:
                raise R2RException(status_code=404, message="Document not found")

            # If document exists, proceed with the assignment
            assign_query = f"""
                UPDATE {self._get_table_name("documents")}
                SET collection_ids = array_append(collection_ids, $1)
                WHERE id = $2 AND NOT ($1 = ANY(collection_ids))
                RETURNING id
            """
            result = await self.connection_manager.fetchrow_query(
                assign_query, [collection_id, document_id]
            )

            if not result:
                # Document exists but was already assigned to the collection
                raise R2RException(
                    status_code=409,
                    message="Document is already assigned to the collection",
                )

            # Update document count for the collection and all its parent collections
            update_collection_query = f"""
                WITH RECURSIVE collection_hierarchy AS (
                    -- Base case: start with the target collection
                    SELECT id, parent_id
                    FROM {self._get_table_name("collections")}
                    WHERE id = $1
                    
                    UNION
                    
                    -- Recursive case: get all parent collections
                    SELECT c.id, c.parent_id
                    FROM {self._get_table_name("collections")} c
                    INNER JOIN collection_hierarchy ch ON c.id = ch.parent_id
                )
                UPDATE {self._get_table_name("collections")} c
                SET document_count = document_count + 1
                WHERE c.id IN (SELECT id FROM collection_hierarchy)
            """
            await self.connection_manager.execute_query(
                query=update_collection_query, params=[collection_id]
            )

            return collection_id

        except R2RException:
            # Re-raise R2RExceptions as they are already handled
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"An error '{e}' occurred while assigning the document to the collection",
            ) from e

    async def remove_document_from_collection_relational(
        self, document_id: UUID, collection_id: UUID
    ) -> None:
        """
        Remove a document from a collection and update document counts for the collection
        and all its parent collections.
        """
        if not await self.collection_exists(collection_id):
            raise R2RException(status_code=404, message="Collection not found")

        query = f"""
            UPDATE {self._get_table_name("documents")}
            SET collection_ids = array_remove(collection_ids, $1)
            WHERE id = $2 AND $1 = ANY(collection_ids)
            RETURNING id
        """
        result = await self.connection_manager.fetchrow_query(
            query, [collection_id, document_id]
        )

        if not result:
            raise R2RException(
                status_code=404,
                message="Document not found in the specified collection",
            )

        # Update document count for the collection and all its parent collections
        update_collection_query = f"""
            WITH RECURSIVE collection_hierarchy AS (
                -- Base case: start with the target collection
                SELECT id, parent_id
                FROM {self._get_table_name("collections")}
                WHERE id = $1
                
                UNION
                
                -- Recursive case: get all parent collections
                SELECT c.id, c.parent_id
                FROM {self._get_table_name("collections")} c
                INNER JOIN collection_hierarchy ch ON c.id = ch.parent_id
            )
            UPDATE {self._get_table_name("collections")} c
            SET document_count = document_count - 1
            WHERE c.id IN (SELECT id FROM collection_hierarchy)
        """
        await self.connection_manager.execute_query(
            query=update_collection_query, params=[collection_id]
        )

    async def decrement_collection_document_count(
        self, collection_id: UUID, decrement_by: int = 1
    ) -> None:
        """
        Decrement the document count for a collection.

        Args:
            collection_id (UUID): The ID of the collection to update
            decrement_by (int): Number to decrease the count by (default: 1)
        """
        collection_query = f"""
            UPDATE {self._get_table_name('collections')}
            SET document_count = document_count - $1
            WHERE id = $2
        """
        await self.connection_manager.execute_query(
            collection_query, [decrement_by, collection_id]
        )

    async def export_to_csv(
        self,
        columns: Optional[list[str]] = None,
        filters: Optional[dict] = None,
        include_header: bool = True,
    ) -> tuple[str, IO]:
        """
        Creates a CSV file from the PostgreSQL data and returns the path to the temp file.
        """
        valid_columns = {
            "id",
            "owner_id",
            "name",
            "description",
            "graph_sync_status",
            "graph_cluster_status",
            "created_at",
            "updated_at",
            "user_count",
            "document_count",
        }

        if not columns:
            columns = list(valid_columns)
        elif invalid_cols := set(columns) - valid_columns:
            raise ValueError(f"Invalid columns: {invalid_cols}")

        select_stmt = f"""
            SELECT
                id::text,
                owner_id::text,
                name,
                description,
                graph_sync_status,
                graph_cluster_status,
                to_char(created_at, 'YYYY-MM-DD HH24:MI:SS') AS created_at,
                to_char(updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at,
                user_count,
                document_count
            FROM {self._get_table_name(self.TABLE_NAME)}
        """

        params = []
        if filters:
            conditions = []
            param_index = 1

            for field, value in filters.items():
                if field not in valid_columns:
                    continue

                if isinstance(value, dict):
                    for op, val in value.items():
                        if op == "$eq":
                            conditions.append(f"{field} = ${param_index}")
                            params.append(val)
                            param_index += 1
                        elif op == "$gt":
                            conditions.append(f"{field} > ${param_index}")
                            params.append(val)
                            param_index += 1
                        elif op == "$lt":
                            conditions.append(f"{field} < ${param_index}")
                            params.append(val)
                            param_index += 1
                else:
                    # Direct equality
                    conditions.append(f"{field} = ${param_index}")
                    params.append(value)
                    param_index += 1

            if conditions:
                select_stmt = f"{select_stmt} WHERE {' AND '.join(conditions)}"

        select_stmt = f"{select_stmt} ORDER BY created_at DESC"

        temp_file = None
        try:
            temp_file = tempfile.NamedTemporaryFile(
                mode="w", delete=True, suffix=".csv"
            )
            writer = csv.writer(temp_file, quoting=csv.QUOTE_ALL)

            async with self.connection_manager.pool.get_connection() as conn:  # type: ignore
                async with conn.transaction():
                    cursor = await conn.cursor(select_stmt, *params)

                    if include_header:
                        writer.writerow(columns)

                    chunk_size = 1000
                    while True:
                        rows = await cursor.fetch(chunk_size)
                        if not rows:
                            break
                        for row in rows:
                            row_dict = {
                                "id": row[0],
                                "owner_id": row[1],
                                "name": row[2],
                                "description": row[3],
                                "graph_sync_status": row[4],
                                "graph_cluster_status": row[5],
                                "created_at": row[6],
                                "updated_at": row[7],
                                "user_count": row[8],
                                "document_count": row[9],
                            }
                            writer.writerow([row_dict[col] for col in columns])

            temp_file.flush()
            return temp_file.name, temp_file

        except Exception as e:
            if temp_file:
                temp_file.close()
            raise HTTPException(
                status_code=500,
                detail=f"Failed to export data: {str(e)}",
            ) from e
