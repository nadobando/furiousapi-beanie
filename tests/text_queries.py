# ruff: noqa: W291

DEEP_FILTER_AND_ROOT = {
    "params": (
        "select(_id);and(like(foreign.inner.name,'a%'),eq(another_id,1))",
        {
            "filter": {
                "$and": [
                    {"foreign.inner.name": {"$regularExpression": {"options": "u", "pattern": "a%"}}},
                    {"another_id": 1},
                ]
            },
            "projection": {"_id": 1},
            "sort": {"_id": 1},
        },
    ),
    "id": "deep_filter_and_root",
}

NESTED_PROJECTION = {
    "params": (
        "select(_id,foreign[inner[name]])",
        {"filter": {}, "projection": {"_id": 1, "foreign": {"inner": {"name": 1}}}, "sort": {"_id": 1}},
    ),
    "id": "nested_projection",
}

DEEP_LIKE = {
    "params": (
        "select(id);like(foreign.inner.name,'b%')",
        {
            "filter": {"foreign.inner.name": {"$regularExpression": {"options": "u", "pattern": "b%"}}},
            "projection": {"_id": 1},
            "sort": {"_id": 1},
        },
    ),
    "id": "deep_like",
}

OR_EQS = {
    "params": (
        "select(id);or(eq(foreign.name,'aaa'),eq(foreign.name,'bbb'))",
        {
            "filter": {"$or": [{"foreign.name": "aaa"}, {"foreign.name": "bbb"}]},
            "projection": {"_id": 1},
            "sort": {"_id": 1},
        },
    ),
    "id": "or_eqs",
}

NOT_EQS = {
    "params": (
        "select(id);not(eq(foreign.name,'aaa'))",
        {"filter": {"foreign.name": {"$not": {"$eq": "aaa"}}}, "projection": {"_id": 1}, "sort": {"_id": 1}},
    ),
    "id": "not_eqs",
}

BOOLEAN_EQ = {
    "params": (
        "select(id);eq(is_boolean,true)",
        {"filter": {"is_boolean": True}, "projection": {"_id": 1}, "sort": {"_id": 1}},
    ),
    "id": "boolean_eq",
}

SELECT_NESTED_FIELD = {
    "params": (
        "select(id,foreign[name])",
        {"filter": {}, "projection": {"_id": 1, "foreign": {"name": 1}}, "sort": {"_id": 1}},
    ),
    "id": "select_nested_field",
}

IN_LIKE_JOIN = {
    "params": (
        "select(id);and(in(_id,(1,2,3)),like(foreign.name,'asda'))",
        {
            "filter": {
                "$and": [
                    {"_id": {"$in": [1, 2, 3]}},
                    {"foreign.name": {"$regularExpression": {"options": "u", "pattern": "asda"}}},
                ]
            },
            "projection": {"_id": 1},
            "sort": {"_id": 1},
        },
    ),
    "id": "in-like-join",
}

DEEP_NESTED_WITH_SORT_AND_OR = {
    "params": (
        "select(_id,foreign[_id,inner]);sort(foreign.id);or(in(_id,(1,2,3)),like(foreign.name,'name'))",
        {
            "filter": {
                "$or": [
                    {"_id": {"$in": [1, 2, 3]}},
                    {"foreign.name": {"$regularExpression": {"options": "u", "pattern": "name"}}},
                ]
            },
            "projection": {"_id": 1, "foreign": {"_id": 1, "inner": {"name": 1}}},
            "sort": {"_id": 1, "foreign._id": 1},
        },
    ),
    "id": "deep_nested_with_sort_and_or",
}

# ruff: noqa: W291


DISTINCT_SIMPLE = {
    "params": ("select(id);distinct(id)", "SELECT my_model.id \nFROM my_model"),
    "id": "distinct_simple",
}

SORT_DESC = {
    "params": ("select(id);sort(-id)", {"filter": {}, "projection": {"_id": 1}, "sort": {"_id": -1}}),
    "id": "sort_desc",
}

SORT_ASC = {
    "params": ("select(id);sort(+id)", {"filter": {}, "projection": {"_id": 1}, "sort": {"_id": 1}}),
    "id": "sort_asc",
}

DEEP_FILTER = {
    "params": (
        "select(_id);eq(foreign.inner.name,'2')",
        {"filter": {"foreign.inner.name": "2"}, "projection": {"_id": 1}, "sort": {"_id": 1}},
    ),
    "id": "deep_filter",
}

DEEP_FILTER_WITH_OR = {
    "params": (
        "select(id);or(eq(int_number,2),eq(nullable,5))",
        {"filter": {"$or": [{"int_number": 2}, {"nullable": 5}]}, "projection": {"_id": 1}, "sort": {"_id": 1}},
    ),
    "id": "deep_filter_with_or",
}

DEEP_FILTER_WITH_AND = {
    "params": (
        "select(id);and(eq(int_number,2),eq(nullable,5))",
        {"filter": {"$and": [{"int_number": 2}, {"nullable": 5}]}, "projection": {"_id": 1}, "sort": {"_id": 1}},
    ),
    "id": "deep_filter_with_and",
}

LIKE_DEEP_FIELD = {
    "params": (
        "select(id);like(foreign.inner.name,'furious.*')",
        {
            "filter": {"foreign.inner.name": {"$regularExpression": {"options": "u", "pattern": "furious.*"}}},
            "projection": {"_id": 1},
            "sort": {"_id": 1},
        },
    ),
    "id": "like_deep_field",
}

SELECT_MANY_TO_MANY = {
    "params": (
        "select(id,many_to_many[*])",
        (
            "SELECT manytomanymodel_1.id, manytomanymodel_1.name, my_model.id AS id_1 \n"
            "FROM my_model "
            "LEFT OUTER JOIN "
            "(manytomanylinkmodel AS manytomanylinkmodel_1 "
            "JOIN manytomanymodel AS manytomanymodel_1 ON manytomanymodel_1.id = manytomanylinkmodel_1.many_id) "
            "ON my_model.id = manytomanylinkmodel_1.model_id"
        ),
    ),
    "id": "select_many_to_many",
}

SELECT_ALL_FIELDS = {
    "params": (
        "select(*)",
        {
            "filter": {},
            "projection": {
                "_id": 1,
                "another_id": 1,
                "created_at": 1,
                "float_number": 1,
                "foreign": 1,
                "inner": 1,
                "int_number": 1,
                "is_boolean": 1,
                "nullable": 1,
                "revision_id": 1,
            },
            "sort": {"_id": 1},
        },
    ),
    "id": "select_all_fields",
}

NESTED_MULTILEVEL_PROJECTION = {
    "params": (
        "select(_id,foreign[inner[name]])",
        {"filter": {}, "projection": {"_id": 1, "foreign": {"inner": {"name": 1}}}, "sort": {"_id": 1}},
    ),
    "id": "nested_multilevel_projection",
}

AND_OR_COMBINATION = {
    "params": (
        "select(id);and(or(eq(nullable,1),eq(nullable,2)),eq(is_boolean,true))",
        {
            "filter": {"$and": [{"$or": [{"nullable": 1}, {"nullable": 2}]}, {"is_boolean": True}]},
            "projection": {"_id": 1},
            "sort": {"_id": 1},
        },
    ),
    "id": "and_or_combination",
}

FILTER_ON_CHILD = {
    "params": (
        "select(id);eq(inner.name,'5')",
        {"filter": {"inner.name": "5"}, "projection": {"_id": 1}, "sort": {"_id": 1}},
    ),
    "id": "filter_on_child",
}

SELECT_CHILD_PROJECTION = {
    "params": (
        "select(id,children[id])",
        (
            "SELECT my_model_1.id, my_model.id AS id_1 \n"
            "FROM my_model "
            "LEFT OUTER JOIN my_model AS my_model_1 ON my_model.id = my_model_1.parent_id"
        ),
    ),
    "id": "select_child_projection",
}

SELECT_CHILD_WILDCARD = {
    "params": (
        "select(id,children[*])",
        (
            "SELECT"
            " my_model_1.created_at,"
            " my_model_1.another_id,"
            " my_model_1.int_number,"
            " my_model_1.float_number,"
            " my_model_1.is_boolean,"
            " my_model_1.nullable,"
            " my_model_1.foreign_id1,"
            " my_model_1.foreign_id2,"
            " my_model_1.id, my_model_1.parent_id,"
            " my_model.id AS id_1 \n"
            "FROM my_model "
            "LEFT OUTER JOIN my_model AS my_model_1 ON my_model.id = my_model_1.parent_id"
        ),
    ),
    "id": "select_child_wildcard",
}

FILTER_NULL = {
    "params": (
        "select(id);eq(nullable,null)",
        {"filter": {"nullable": None}, "projection": {"_id": 1}, "sort": {"_id": 1}},
    ),
    "id": "filter_null",
}

MULTI_FIELD_SORT = {
    "params": (
        "select(id);sort(+int_number,-float_number)",
        {
            "filter": {},
            "projection": {"_id": 1},
            "sort": {
                "_id": -1,
                "int_number": 1,
                "float_number": -1,
            },
        },
    ),
    "id": "multi_field_sort",
}
MULTI_FIELD_NESTED_SORT = {
    "params": (
        "select(id);sort(+foreign.id,-foreign.name)",
        {"filter": {}, "projection": {"_id": 1}, "sort": {"_id": -1, "foreign._id": 1, "foreign.name": -1}},
    ),
    "id": "multi_field_nested_sort",
}

SEARCH_STRING_FIELD = {
    "params": (
        "select(id);like(foreign.name,'%abc%')",
        {
            "filter": {"foreign.name": {"$regularExpression": {"options": "u", "pattern": "%abc%"}}},
            "projection": {"_id": 1},
            "sort": {"_id": 1},
        },
    ),
    "id": "search_string_field",
}

NESTED_FILTER_AND_SORT = {
    "params": (
        "select(id,foreign[inner[name]]);eq(foreign.inner.name,'x');sort(+foreign.inner.name)",
        {
            "filter": {"foreign.inner.name": "x"},
            "projection": {"_id": 1, "foreign": {"inner": {"name": 1}}},
            "sort": {"_id": 1, "foreign.inner.name": 1},
        },
    ),
    "id": "nested_filter_and_sort",
}
NESTED_WILDCARD_AND_FIELD_PROJECTION = {
    "params": (
        "select(_id,foreign[*,inner[name]])",
        {
            "filter": {},
            "projection": {"_id": 1, "foreign": {"_id": 1, "inner": {"name": 1}, "name": 1, "revision_id": 1}},
            "sort": {"_id": 1},
        },
    ),
    "id": "nested_wildcard_and_field_projection",
}

FILTER_DEEP_FIELD_SORT_ROOT = {
    "params": (
        "select(id);eq(foreign.inner.name,'abc');sort(+id)",
        {"filter": {"foreign.inner.name": "abc"}, "projection": {"_id": 1}, "sort": {"_id": 1}},
    ),
    "id": "filter_deep_field_sort_root",
}

SELECT_DEEP_FIELD_ONLY = {
    "params": (
        "select(foreign[inner[name]]);eq(foreign.inner.name,'abc')",
        {
            "filter": {"foreign.inner.name": "abc"},
            "projection": {"foreign": {"inner": {"name": 1}}},
            "sort": {"_id": 1},
        },
    ),
    "id": "select_deep_field_only",
}


INNER_DOC = {
    "params": (
        "eq(inner.name,'a')",
        {
            "filter": {"inner.name": "a"},
            "projection": {
                "_id": 1,
                "another_id": 1,
                "created_at": 1,
                "float_number": 1,
                "foreign": 1,
                "inner": 1,
                "int_number": 1,
                "is_boolean": 1,
                "nullable": 1,
                "revision_id": 1,
            },
            "sort": {"_id": 1},
        },
    ),
    "id": "inner_doc",
}
WILDCARD_AND_EXPLICIT_NESTED_SORT = {
    "params": (
        "select(_id,foreign[*]);sort(+foreign.inner.name)",
        {
            "filter": {},
            "projection": {"_id": 1, "foreign": {"_id": 1, "inner": 1, "name": 1, "revision_id": 1}},
            "sort": {"_id": 1, "foreign.inner.name": 1},
        },
    ),
    "id": "wildcard_and_explicit_nested_sort",
}
# ruff: noqa: W291

ALL_TEST_CASES = [
    AND_OR_COMBINATION,
    BOOLEAN_EQ,
    DEEP_FILTER,
    DEEP_FILTER_AND_ROOT,
    DEEP_FILTER_WITH_AND,
    DEEP_FILTER_WITH_OR,
    DEEP_LIKE,
    DEEP_NESTED_WITH_SORT_AND_OR,
    FILTER_DEEP_FIELD_SORT_ROOT,
    FILTER_NULL,
    FILTER_ON_CHILD,
    IN_LIKE_JOIN,
    INNER_DOC,
    LIKE_DEEP_FIELD,
    MULTI_FIELD_NESTED_SORT,
    MULTI_FIELD_SORT,
    NESTED_FILTER_AND_SORT,
    NESTED_MULTILEVEL_PROJECTION,
    NESTED_PROJECTION,
    NESTED_WILDCARD_AND_FIELD_PROJECTION,
    NOT_EQS,
    OR_EQS,
    SEARCH_STRING_FIELD,
    SELECT_ALL_FIELDS,
    SELECT_CHILD_PROJECTION,
    SELECT_CHILD_WILDCARD,
    SELECT_DEEP_FIELD_ONLY,
    SELECT_NESTED_FIELD,
    SORT_ASC,
    SORT_DESC,
    WILDCARD_AND_EXPLICIT_NESTED_SORT,
]
