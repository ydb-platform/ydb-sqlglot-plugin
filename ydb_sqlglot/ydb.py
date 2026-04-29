import inspect as _inspect
import re
import typing as t
from collections import defaultdict

from sqlglot import Generator, TokenType, exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy, concat_to_dpipe_sql, unit_to_var
from sqlglot.errors import UnsupportedError
from sqlglot.expressions import Expression
from sqlglot.helper import flatten, name_sequence, seq_get
from sqlglot.optimizer.scope import ScopeType, find_in_scope, traverse_scope
from sqlglot.optimizer.simplify import simplify
from sqlglot.transforms import eliminate_join_marks, move_ctes_to_top_level

JOIN_ATTRS = ("on", "side", "kind", "using", "method")


def rename_func_not_normalize(name: str) -> t.Callable[[Generator, exp.Expression], str]:
    return lambda self, expression: self.func(
        name, *flatten(expression.args.values()), normalize=False
    )


def table_names_to_lower_case(expression: exp.Expression) -> exp.Expression:
    for table in expression.find_all(exp.Table):
        if isinstance(table.this, exp.Identifier):
            ident = table.this
            table.set("this", ident.this.lower())
    return expression


def make_db_name_lower(expression: exp.Expression) -> exp.Expression:
    """
    Converts all database names to uppercase

    Args:
        expression: The SQL expression to modify

    Returns:
        Modified expression with uppercase database names
    """
    for table in expression.find_all(exp.Table):
        if table.db:
            table.set("db", table.db.lower())

    return expression


def apply_alias_to_select_from_table(expression: exp.Expression) -> Expression:
    """
    Applies aliases to columns in SELECT statements that reference tables

    Args:
        expression: The SQL expression to modify

    Returns:
        Modified expression with aliases applied to columns
    """
    for column in expression.find_all(exp.Column):
        if not isinstance(column.this, exp.Star):
            if hasattr(column, "table") and column.table and len(column.table) > 0:
                if isinstance(column.parent, exp.Select):
                    column.replace(exp.alias_(column, column.alias_or_name))
    return expression


def _replace(expression, condition):
    """
    Helper function to replace an expression with a condition

    Args:
        expression: The expression to replace
        condition: The condition to replace with

    Returns:
        The replaced expression
    """
    return expression.replace(exp.condition(condition))


_DATE_LITERAL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_agg_alias_seq = iter(range(10_000))


def _alias_order_by_aggregates(expression: exp.Expression) -> None:
    """
    YDB does not allow ORDER BY on an aggregate expression directly
    (e.g. ORDER BY COUNT(*) DESC). Fix: for every SELECT that has an
    unaliased aggregate in ORDER BY which also appears in the SELECT
    list, give that aggregate an auto-generated alias and replace the
    ORDER BY reference with it. Mutates in-place.
    """
    for select in list(expression.find_all(exp.Select)):
        order = select.args.get("order")
        if not order:
            continue
        for ordered in order.expressions:
            ob_expr = ordered.this
            if not isinstance(ob_expr, exp.AggFunc):
                continue
            ob_sql = ob_expr.sql()
            # find matching SELECT expression
            for sel_expr in select.expressions:
                candidate = sel_expr.this if isinstance(sel_expr, exp.Alias) else sel_expr
                if candidate.sql() == ob_sql:
                    if isinstance(sel_expr, exp.Alias):
                        alias_name = sel_expr.alias
                    else:
                        alias_name = f"_agg_{next(_agg_alias_seq)}"
                        new_alias = exp.Alias(this=sel_expr.copy(), alias=exp.to_identifier(alias_name))
                        sel_expr.replace(new_alias)
                    ordered.set("this", exp.column(alias_name))
                    break


def _expand_positional_group_by(expression: exp.Expression) -> None:
    """
    YDB rejects GROUP BY with integer literals (positional references).
    Replace each integer literal in GROUP BY with the corresponding SELECT
    expression. Mutates in-place.
    """
    for select in list(expression.find_all(exp.Select)):
        group = select.args.get("group")
        if not group:
            continue
        sel_exprs = select.expressions
        new_group_exprs = []
        changed = False
        for g in group.expressions:
            if isinstance(g, exp.Literal) and not g.is_string:
                idx = int(g.this) - 1  # 1-based → 0-based
                if 0 <= idx < len(sel_exprs):
                    sel = sel_exprs[idx]
                    expanded = sel.this.copy() if isinstance(sel, exp.Alias) else sel.copy()
                    # Skip constant/literal expressions — they don't affect grouping
                    if isinstance(expanded, exp.Literal):
                        changed = True
                        continue
                    new_group_exprs.append(expanded)
                    changed = True
                    continue
            new_group_exprs.append(g)
        if changed:
            group.set("expressions", new_group_exprs)


_subq_alias_seq = iter(range(10_000))


def _wrap_udf_group_by(expression: exp.Expression) -> None:
    """
    YDB does not allow GROUP BY expressions that reference columns not directly
    listed in GROUP BY.  E.g. GROUP BY DateTime::GetMinute(EventTime) fails
    because EventTime itself is not in GROUP BY.

    Fix: when a GROUP BY contains a non-trivial expression (function call, CASE, …)
    — either directly or via a SELECT alias reference — wrap the whole SELECT in a
    subquery that materialises those expressions as aliases, then GROUP BY the plain
    aliases in the outer query.

    Mutates the tree in-place (replaces the Select node).
    """
    def _is_trivial(e: exp.Expression) -> bool:
        return isinstance(e, (exp.Column, exp.Identifier))

    for select in list(expression.find_all(exp.Select)):
        group = select.args.get("group")
        if not group:
            continue

        # Build alias → underlying_expression map from the SELECT clause.
        alias_to_expr: dict[str, exp.Expression] = {}
        for sel in select.expressions:
            if isinstance(sel, exp.Alias):
                alias_to_expr[sel.alias] = sel.this

        # Resolve each GROUP BY expression to its "effective" form
        # (expand alias references to the aliased expression).
        def _effective(e: exp.Expression) -> exp.Expression:
            if isinstance(e, exp.Alias):
                return e.this
            if isinstance(e, exp.Column):
                name = e.name
                if name in alias_to_expr:
                    return alias_to_expr[name]
            return e

        # Check: does any effective GROUP BY expression need the subquery treatment?
        has_complex = any(not _is_trivial(_effective(g)) for g in group.expressions)
        if not has_complex:
            continue

        # Resolve GROUP BY: replace alias-references with their underlying expressions.
        resolved_group: list[exp.Expression] = [_effective(g) for g in group.expressions]

        # For each non-trivial resolved GROUP BY expression, find or assign an alias.
        gb_alias: dict[str, str] = {}  # effective_expr.sql() → alias_name
        new_sel_exprs = list(select.expressions)

        for eff in resolved_group:
            if _is_trivial(eff):
                continue
            eff_sql = eff.sql()
            # Find an existing SELECT alias for this expression.
            found = next(
                (sel.alias for sel in select.expressions
                 if isinstance(sel, exp.Alias) and sel.this.sql() == eff_sql),
                None,
            )
            if found:
                gb_alias[eff_sql] = found
            else:
                new_name = f"_gb_{next(_subq_alias_seq)}"
                gb_alias[eff_sql] = new_name
                new_sel_exprs.append(
                    exp.Alias(this=eff.copy(), alias=exp.to_identifier(new_name))
                )

        # Collect raw column references used inside aggregate functions in the outer SELECT.
        # These columns must also be available in the subquery for the outer aggregates.
        agg_cols: set[str] = set()
        for sel in new_sel_exprs:
            agg_search = sel.this if isinstance(sel, exp.Alias) else sel
            if isinstance(agg_search, exp.AggFunc):
                for col in agg_search.find_all(exp.Column):
                    col_name = col.name
                    if col_name not in alias_to_expr:  # not a SELECT alias
                        agg_cols.add(col_name)

        # Build the inner subquery SELECT (no aggregates, no GROUP BY / HAVING).
        inner_sel_exprs = []
        seen_inner_names: set[str] = set()
        for sel in new_sel_exprs:
            inner_expr = sel.this if isinstance(sel, exp.Alias) else sel
            if isinstance(inner_expr, exp.AggFunc):
                continue  # aggregates belong in the outer query
            inner_sel_exprs.append(sel.copy())
            alias_nm = sel.alias if isinstance(sel, exp.Alias) else sel.name
            seen_inner_names.add(alias_nm)

        # Add raw column refs needed by outer aggregates if not already in inner SELECT.
        for col_name in sorted(agg_cols):
            if col_name not in seen_inner_names:
                inner_sel_exprs.append(exp.column(col_name))
                seen_inner_names.add(col_name)

        inner_select = exp.Select(
            expressions=inner_sel_exprs,
            **{
                k: v.copy() if v is not None else None
                for k, v in select.args.items()
                if k not in ("expressions", "group", "having", "order",
                             "limit", "offset", "distinct", "operation_modifiers")
            }
        )
        subq_alias = f"_subq_{next(_subq_alias_seq)}"
        subquery = exp.Subquery(
            this=inner_select,
            alias=exp.TableAlias(this=exp.to_identifier(subq_alias)),
        )

        # Outer GROUP BY: use alias name whenever the original GB item referenced a
        # SELECT alias (trivial or not); otherwise keep the resolved expression.
        new_group_exprs = []
        for g, eff in zip(group.expressions, resolved_group):
            if isinstance(g, exp.Column) and g.name in alias_to_expr:
                # GB item was an alias reference → use the alias name in the outer query
                new_group_exprs.append(exp.column(g.name))
            elif _is_trivial(eff):
                new_group_exprs.append(eff.copy())
            else:
                new_group_exprs.append(exp.column(gb_alias[eff.sql()]))

        # Outer SELECT expressions: replace non-trivial expressions with alias columns,
        # keep aggregates as-is.
        orig_alias_names = {sel.alias for sel in select.expressions if isinstance(sel, exp.Alias)}

        def _outer_sel(sel: exp.Expression) -> t.Optional[exp.Expression]:
            if isinstance(sel, exp.Alias):
                # Synthesised GB alias not in original SELECT → skip
                if sel.alias.startswith("_gb_") and sel.alias not in orig_alias_names:
                    return None
                inner = sel.this
                if isinstance(inner, exp.AggFunc):
                    return sel.copy()
                return exp.column(sel.alias)
            return sel.copy()

        outer_sel_exprs = [r for sel in new_sel_exprs if (r := _outer_sel(sel)) is not None]

        # Reconstruct the outer SELECT (keep HAVING, ORDER BY, LIMIT, etc.).
        outer_kwargs = {
            k: v.copy() if v is not None else None
            for k, v in select.args.items()
            if k not in ("expressions", "from_", "joins", "where", "group",
                         "prewhere", "laterals", "pivots", "match", "connect", "start")
        }
        outer_kwargs["expressions"] = outer_sel_exprs
        outer_kwargs["from_"] = exp.From(this=subquery)
        outer_kwargs["group"] = exp.Group(expressions=new_group_exprs)

        outer_select = exp.Select(**outer_kwargs)
        if select.parent is None:
            # Root node — replace() won't work; mutate args in-place.
            select.args.clear()
            select.args.update(outer_select.args)
        else:
            select.replace(outer_select)


def _cast_date_string_literals(expression: exp.Expression) -> None:
    """
    Wrap ISO date string literals (YYYY-MM-DD) in CAST(... AS DATE) when they
    appear in comparison contexts. YDB requires explicit typing; it does not
    coerce bare strings to DATE automatically.
    Mutates the expression in-place.
    """
    for node in list(expression.find_all(exp.GTE, exp.LTE, exp.GT, exp.LT, exp.EQ, exp.NEQ)):
        for side in ("this", "expression"):
            child = node.args.get(side)
            if (
                isinstance(child, exp.Literal)
                and child.is_string
                and _DATE_LITERAL_RE.match(child.this)
            ):
                node.set(
                    side,
                    exp.Cast(
                        this=child.copy(),
                        to=exp.DataType(this=exp.DataType.Type.DATE, nested=False),
                    ),
                )


def _other_operand(expression):
    """
    Returns the other operand of a binary operation involving a subquery

    Args:
        expression: The expression containing a binary operation

    Returns:
        The operand that is not a subquery, or None
    """
    if isinstance(expression, exp.In):
        return expression.this

    if isinstance(expression, (exp.Any, exp.All)):
        return _other_operand(expression.parent)

    if isinstance(expression, exp.Binary):
        return (
            expression.right
            if isinstance(expression.left, (exp.Subquery, exp.Any, exp.Exists, exp.All))
            else expression.left
        )

    return None


def _simplify_double_not(expression: exp.Expression) -> None:
    """Simplify NOT NOT x → x in-place.

    YDB does not accept ``NOT NOT (expr)`` syntax.  This pattern can appear
    after subquery unnesting when an EXISTS predicate is wrapped in a NOT IN
    context, producing a double negation like ``NOT NOT (_u_1.key IS NULL)``.
    We remove both NOTs to restore the original semantics.
    """
    for node in list(expression.walk()):
        if isinstance(node, exp.Not) and isinstance(node.this, exp.Not):
            node.replace(node.this.this.copy())


def _apply_subquery_alias_columns(expression: exp.Expression) -> None:
    """Apply subquery alias column names to the SELECT columns in-place.

    SQL allows: ``(SELECT a, b FROM ...) AS t (x, y)`` — aliases ``x`` and ``y``
    rename the SELECT's output columns.  YDB does not support this column-list
    syntax on subquery aliases (``SUPPORTS_TABLE_ALIAS_COLUMNS = False``), so we
    inline the aliases directly on the SELECT expressions before generation.
    """
    for subquery in list(expression.find_all(exp.Subquery)):
        alias = subquery.args.get("alias")
        if not isinstance(alias, exp.TableAlias):
            continue
        col_list = alias.columns
        if not col_list:
            continue
        inner = subquery.this
        if not isinstance(inner, exp.Select):
            continue
        if len(col_list) != len(inner.expressions):
            continue
        new_exprs = []
        for sel_expr, col_id in zip(inner.expressions, col_list):
            alias_name = col_id.name if hasattr(col_id, "name") else str(col_id)
            if not alias_name:
                new_exprs.append(sel_expr)
                continue
            if isinstance(sel_expr, exp.Alias):
                new_sel = sel_expr.copy()
                new_sel.set("alias", exp.to_identifier(alias_name))
            else:
                new_sel = exp.alias_(sel_expr.copy(), alias_name)
            new_exprs.append(new_sel)
        inner.set("expressions", new_exprs)
        alias.set("columns", [])


class FlattenBy(exp.Expression):
    """YDB-specific FLATTEN [LIST|DICT] BY clause on a table reference."""
    arg_types = {"this": True, "expressions": True, "kind": False}


class AssumeOrderBy(exp.Expression):
    """YDB-specific ASSUME ORDER BY hint (data is pre-sorted, skip sort)."""
    arg_types = {"this": True}


class YdbTuple(exp.Expression):
    """YDB Tuple<T1, T2, ...> type — positional unnamed fields."""
    arg_types = {"expressions": True, "nullable": False}


class YdbAtString(exp.Expression):
    """YDB @@...@@ string literal."""
    arg_types = {"this": True}


class YdbPostfixCall(exp.Expression):
    """YDB call of an expression result, e.g. $grep(x) or DateTime::Format(fmt)(ts)."""
    arg_types = {"this": True, "expressions": False}


class YdbJsonValue(exp.Expression):
    """YDB JSON_VALUE with PASSING, RETURNING, ON EMPTY, and ON ERROR clauses."""
    arg_types = {
        "this": True,
        "path": True,
        "passing": False,
        "returning": False,
        "on_empty": False,
        "on_error": False,
    }


class YdbJsonQuery(exp.Expression):
    """YDB JSON_QUERY with PASSING, WRAPPER, ON EMPTY, and ON ERROR clauses."""
    arg_types = {
        "this": True,
        "path": True,
        "passing": False,
        "wrapper": False,
        "on_empty": False,
        "on_error": False,
    }


class YdbJsonExists(exp.Expression):
    """YDB JSON_EXISTS with PASSING and ON ERROR clauses."""
    arg_types = {
        "this": True,
        "path": True,
        "passing": False,
        "on_error": False,
    }


class YdbJsonPassingItem(exp.Expression):
    """One JSON PASSING item: <expression> AS <variable name>."""
    arg_types = {"this": True, "alias": False, "quoted": False}


class YdbLambdaBlock(exp.Expression):
    """YDB lambda body with local named expressions followed by RETURN."""
    arg_types = {"this": True, "expressions": False}


# Container types that use Generic<T, ...> syntax in YDB
_YDB_GENERIC_TYPES = {
    "List": exp.DataType.Type.LIST,
    "Dict": exp.DataType.Type.MAP,
    "Set": exp.DataType.Type.SET,
}


# sqlglot >= 30.0.0 changed Parser.expression() to take a pre-built instance instead of
# (cls, **kwargs). Detect once so the YDB parser override below can support both APIs.
_EXPRESSION_TAKES_INSTANCE = (
    "instance" in _inspect.signature(parser.Parser.expression).parameters
)


def _reassemble_ctes(
    statements: t.List[t.Optional[exp.Expression]],
) -> t.List[t.Optional[exp.Expression]]:
    """Convert sequences of YDB named-expression statements into standard WITH CTEs.

    YDB generator emits:  $t = (SELECT ...);  SELECT * FROM $t AS t
    This function rebuilds:  WITH t AS (SELECT ...) SELECT * FROM t

    so that transpiling YDB output to other dialects produces valid SQL.
    """
    result: t.List[t.Optional[exp.Expression]] = []
    # Keep both the original Alias nodes and the converted CTE nodes
    pending_aliases: t.List[exp.Alias] = []
    pending_ctes: t.List[exp.CTE] = []
    pending_names: t.Set[str] = set()

    def _flush_as_aliases() -> None:
        result.extend(pending_aliases)
        pending_aliases.clear()
        pending_ctes.clear()
        pending_names.clear()

    for stmt in statements:
        if stmt is None:
            continue

        if (
            isinstance(stmt, exp.Alias)
            and isinstance(stmt.args.get("alias"), exp.Identifier)
            and stmt.alias.startswith("$")
        ):
            name = stmt.alias[1:]
            inner = stmt.this
            # Unwrap Subquery — CTE.this must be Select, not Subquery
            if isinstance(inner, exp.Subquery):
                inner = inner.this
            if not isinstance(inner, exp.Query):
                _flush_as_aliases()
                result.append(stmt)
                continue
            # Replace any $prev_cte refs inside this CTE body
            inner = _replace_param_table_refs(inner, pending_names)
            pending_aliases.append(stmt)
            pending_ctes.append(
                exp.CTE(
                    this=inner,
                    alias=exp.TableAlias(this=exp.to_identifier(name)),
                )
            )
            pending_names.add(name)
        elif pending_ctes and isinstance(stmt, exp.Select):
            stmt = _replace_param_table_refs(stmt, pending_names)
            stmt.set("with_", exp.With(expressions=list(pending_ctes)))
            result.append(stmt)
            pending_aliases.clear()
            pending_ctes.clear()
            pending_names.clear()
        else:
            # No following SELECT — keep original Alias form
            _flush_as_aliases()
            result.append(stmt)

    # Trailing named exprs without a SELECT — keep as-is
    _flush_as_aliases()
    return result


def _replace_param_table_refs(
    tree: exp.Expression, names: t.Set[str]
) -> exp.Expression:
    """Replace Table(Parameter(Var("t"))) with Table(Identifier("t")) for CTE names."""

    def _transform(node: exp.Expression) -> exp.Expression:
        if (
            isinstance(node, exp.Table)
            and isinstance(node.this, exp.Parameter)
            and isinstance(node.this.this, exp.Var)
            and node.this.this.name in names
        ):
            return exp.Table(
                this=exp.to_identifier(node.this.this.name),
                alias=node.args.get("alias"),
            )
        return node

    return tree.transform(_transform)


class YDB(Dialect):
    """
    YDB SQL dialect implementation for sqlglot.
    Implements the specific syntax and features of YDB database.
    """

    DATE_FORMAT = "'%Y-%m-%d'"
    TIME_FORMAT = "'%Y-%m-%d %H:%M:%S'"

    TIME_MAPPING = {
        "%Y": "%Y",
        "%m": "%m",
        "%d": "%d",
        "%H": "%H",
        "%M": "%M",
        "%S": "%S",
    }
    NORMALIZE_FUNCTIONS = False
    # YDB handles safe division via NULLIF in the source dialect already;
    # setting this prevents div_sql from wrapping the denominator a second time.
    SAFE_DIVISION = True
    # YDB does not support NULLS FIRST / NULLS LAST; prevent the generator from emitting them.
    NULL_ORDERING = None

    class Tokenizer(tokens.Tokenizer):
        """
        Tokenizer implementation for YDB SQL dialect.
        Defines how the SQL text is broken into tokens.
        """

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "DECLARE": TokenType.DECLARE,
            "UTF8": TokenType.TEXT,       # YDB Utf8 = unicode text = SQL TEXT
            "STRING": TokenType.BLOB,     # YDB String = bytes = SQL BLOB
        }

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.PARAMETER,
        }
        VAR_SINGLE_TOKENS = {"$"}

        SUPPORTS_VALUES_DEFAULT = False
        QUOTES = ["'", '"']
        COMMENTS = ["--", ("/*", "*/")]
        IDENTIFIERS = ["`"]

    class Parser(parser.Parser):
        COLUMN_OPERATORS = {
            **parser.Parser.COLUMN_OPERATORS,
            # In YDB :: is a module namespace separator (e.g. DateTime::GetYear),
            # not a Postgres-style cast. Reparse the right side as a function call.
            TokenType.DCOLON: lambda self, this, field: (
                self.expression(
                    exp.Anonymous(
                        this=f"{this.name}::{field.name}",
                        expressions=field.expressions,
                    )
                )
                if isinstance(field, exp.Func)
                else self.expression(exp.ScopeResolution(this=this, expression=field))
            ),
        }

        LAMBDAS = {
            **parser.Parser.LAMBDAS,
            TokenType.ARROW: lambda self, expressions: self._parse_ydb_lambda(expressions),
        }

        STATEMENT_PARSERS = {
            **parser.Parser.STATEMENT_PARSERS,
            TokenType.DECLARE: lambda self: self._parse_ydb_declare(),
            TokenType.PARAMETER: lambda self: self._parse_ydb_named_expr(),
        }

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "JSON_EXISTS": lambda self: self._parse_ydb_json_exists(),
            "JSON_QUERY": lambda self: self._parse_ydb_json_query(),
            "JSON_VALUE": lambda self: self._parse_ydb_json_value(),
        }

        PRIMARY_PARSERS = {
            **parser.Parser.PRIMARY_PARSERS,
            TokenType.STRING: lambda self, token: self._parse_ydb_string(token),
        }

        def parse(self, raw_tokens, sql=None):
            self.reset()
            self.sql = sql or ""

            chunks: t.List[t.List[tokens.Token]] = [[]]
            brace_depth = 0
            total = len(raw_tokens)

            for i, token in enumerate(raw_tokens):
                if token.token_type == TokenType.L_BRACE:
                    brace_depth += 1
                elif token.token_type == TokenType.R_BRACE and brace_depth:
                    brace_depth -= 1

                if token.token_type == TokenType.SEMICOLON and not brace_depth:
                    if token.comments:
                        chunks.append([token])
                    if i < total - 1:
                        chunks.append([])
                else:
                    chunks[-1].append(token)

            self._chunks = chunks
            statements = self._parse_ydb_chunks(self.__class__._parse_statement, chunks)
            return _reassemble_ctes(statements)

        def _parse_ydb_chunks(self, parse_method, chunks):
            expressions = []

            # sqlglot >= 29 has this helper; 28.x only exposes the lower-level
            # parser state, so keep a local fallback for the supported range.
            if hasattr(self, "_parse_batch_statements"):
                return self._parse_batch_statements(
                    parse_method=parse_method,
                    sep_first_statement=False,
                )

            for chunk in chunks:
                self._index = -1
                self._tokens = chunk
                self._tokens_size = len(chunk)
                self._advance()

                expressions.append(parse_method(self))

                if self._index < len(self._tokens):
                    self.raise_error("Invalid expression / Unexpected token")

                self.check_errors()

            return expressions

        def expression(self, exp_class_or_instance, token=None, comments=None, **kwargs):
            """Bridge sqlglot's two `Parser.expression()` calling conventions.

            sqlglot < 30 expects ``expression(cls, **kwargs)`` and instantiates internally.
            sqlglot >= 30 expects a pre-built ``expression(instance)`` and rejects kwargs.
            Several call sites in this dialect (and a few in upstream code paths we exercise)
            mix both styles, so normalise here before delegating.
            """
            if _EXPRESSION_TAKES_INSTANCE:
                if not isinstance(exp_class_or_instance, exp.Expression):
                    exp_class_or_instance = exp_class_or_instance(**kwargs)
                return super().expression(
                    exp_class_or_instance, token=token, comments=comments
                )

            if isinstance(exp_class_or_instance, exp.Expression):
                # Old super() would attempt instance(**kwargs) -> "object is not callable".
                instance = exp_class_or_instance
                if token is not None:
                    update_positions = getattr(instance, "update_positions", None)
                    if update_positions is not None:
                        update_positions(token)
                if comments:
                    instance.add_comments(comments)
                else:
                    self._add_comments(instance)
                return self.validate_expression(instance)

            return super().expression(
                exp_class_or_instance, token=token, comments=comments, **kwargs
            )

        def _parse_dcolon(self) -> t.Optional[exp.Expression]:
            return self._parse_function(anonymous=True) or self._parse_var(any_token=True)

        def _parse_ydb_named_expr(self) -> t.Optional[exp.Expression]:
            # _match_set already consumed '$', so _index points to the var name.
            # Retreat one extra step to include '$' when falling back to expression parsing.
            index = self._index - 1
            name_var = self._parse_var(any_token=True)
            if not self._match(TokenType.EQ):
                # Not an assignment — retreat (including '$') and parse as expression.
                self._retreat(index)
                return self._parse_expression()
            value = self._parse_select() or self._parse_expression()
            return self.expression(
                exp.Alias(
                    this=value,
                    alias=exp.Identifier(this=f"${name_var.name}"),
                )
            )

        def _parse_lambda_arg(self) -> t.Optional[exp.Expression]:
            if self._match(TokenType.PARAMETER):
                name = self._parse_var(any_token=True)
                if not name:
                    return None

                parameter = exp.Parameter(this=name)
                if self._match(TokenType.PLACEHOLDER):
                    parameter.meta["optional"] = True
                return self.expression(parameter)

            arg = super()._parse_lambda_arg()
            if arg and self._match(TokenType.PLACEHOLDER):
                arg.meta["optional"] = True
            return arg

        def _parse_ydb_declare(self) -> exp.Declare:
            items = self._parse_csv(self._parse_ydb_declareitem)
            declare = self.expression(exp.Declare(expressions=items))

            item_comments = {
                comment
                for item in items
                if item is not None
                for comment in (item.comments or [])
            }
            if item_comments and declare.comments:
                declare.comments = [
                    comment
                    for comment in declare.comments
                    if comment.strip().startswith("!") or comment not in item_comments
                ]

            return declare

        def _parse_ydb_declareitem(self) -> t.Optional[exp.DeclareItem]:
            if not self._match(TokenType.PARAMETER):
                return None
            name = self._parse_var(any_token=True)
            if not name:
                return None
            self._match(TokenType.ALIAS)
            kind = self._parse_types()
            comments = self._prev.comments if self._prev else None
            return self.expression(
                exp.DeclareItem(this=name, kind=kind),
                comments=comments,
            )

        def _parse_ydb_string(self, token: tokens.Token) -> exp.Literal:
            literal = self.expression(exp.Literal(this=token.text, is_string=True), token)
            if (
                self._curr
                and self._curr.token_type == TokenType.VAR
                and self._curr.text.lower() == "u"
                and token.end + 1 == self._curr.start
            ):
                literal.meta["ydb_string_suffix"] = self._curr.text
                self._advance()
            return literal

        def _parse_ydb_json_value(self) -> YdbJsonValue:
            this = self._parse_bitwise()
            self._match(TokenType.COMMA)
            path = self._parse_bitwise()
            passing = self._parse_ydb_json_passing()
            returning = self._match(TokenType.RETURNING) and self._parse_type()
            on_empty = self._parse_ydb_json_on_clause("EMPTY")
            on_error = self._parse_ydb_json_on_clause("ERROR")
            return self.expression(
                YdbJsonValue(
                    this=this,
                    path=path,
                    passing=passing,
                    returning=returning,
                    on_empty=on_empty,
                    on_error=on_error,
                )
            )

        def _parse_ydb_json_exists(self) -> YdbJsonExists:
            this = self._parse_bitwise()
            self._match(TokenType.COMMA)
            path = self._parse_bitwise()
            passing = self._parse_ydb_json_passing()
            on_error = self._parse_ydb_json_exists_on_error()
            return self.expression(
                YdbJsonExists(
                    this=this,
                    path=path,
                    passing=passing,
                    on_error=on_error,
                )
            )

        def _parse_ydb_json_query(self) -> YdbJsonQuery:
            this = self._parse_bitwise()
            self._match(TokenType.COMMA)
            path = self._parse_bitwise()
            passing = self._parse_ydb_json_passing()
            wrapper = self._parse_ydb_json_query_wrapper()
            on_empty = self._parse_ydb_json_query_on_clause("EMPTY")
            on_error = self._parse_ydb_json_query_on_clause("ERROR")
            return self.expression(
                YdbJsonQuery(
                    this=this,
                    path=path,
                    passing=passing,
                    wrapper=wrapper,
                    on_empty=on_empty,
                    on_error=on_error,
                )
            )

        def _parse_ydb_json_passing(self) -> list[exp.Expression]:
            if not (self._curr and self._curr.text.upper() == "PASSING"):
                return []
            self._advance()
            return self._parse_csv(self._parse_ydb_json_passing_item)

        def _parse_ydb_json_passing_item(self) -> t.Optional[exp.Expression]:
            value = self._parse_assignment()
            if not value:
                return None
            self._match(TokenType.ALIAS)
            alias = self._parse_id_var(any_token=True)
            quoted = bool(isinstance(alias, exp.Identifier) and alias.args.get("quoted"))
            return self.expression(
                YdbJsonPassingItem(
                    this=value,
                    alias=alias.name if alias else "",
                    quoted=quoted,
                )
            )

        def _parse_ydb_json_on_clause(self, name: str) -> t.Optional[exp.Expression]:
            if not self._curr:
                return None

            default_value = None
            action = None
            if self._match(TokenType.DEFAULT):
                default_value = self._parse_assignment()
                action = "DEFAULT"
            elif self._match(TokenType.NULL):
                action = "NULL"
            elif self._curr.text.upper() == "ERROR":
                self._advance()
                action = "ERROR"
            else:
                return None

            if not (self._match(TokenType.ON) and self._curr and self._curr.text.upper() == name):
                return None
            self._advance()

            return exp.Var(this=action, expression=default_value)

        def _parse_ydb_json_exists_on_error(self) -> t.Optional[exp.Expression]:
            action = None
            if self._match(TokenType.TRUE):
                action = "TRUE"
            elif self._match(TokenType.FALSE):
                action = "FALSE"
            elif self._match(TokenType.UNKNOWN):
                action = "UNKNOWN"
            elif self._curr and self._curr.text.upper() == "ERROR":
                self._advance()
                action = "ERROR"
            else:
                return None

            if not (self._match(TokenType.ON) and self._curr and self._curr.text.upper() == "ERROR"):
                return None
            self._advance()
            return exp.Var(this=action)

        def _parse_ydb_json_query_wrapper(self) -> t.Optional[exp.Var]:
            if self._curr and self._curr.text.upper() == "WITHOUT":
                self._advance()
                self._match(TokenType.ARRAY)
                if self._curr and self._curr.text.upper() == "WRAPPER":
                    self._advance()
                    return exp.Var(this="WITHOUT ARRAY WRAPPER")
                return None

            if not (self._curr and self._curr.text.upper() == "WITH"):
                return None
            self._advance()

            mode = ""
            if self._curr and self._curr.text.upper() in ("CONDITIONAL", "UNCONDITIONAL"):
                mode = self._curr.text.upper()
                self._advance()
            self._match(TokenType.ARRAY)
            if self._curr and self._curr.text.upper() == "WRAPPER":
                self._advance()
            else:
                return None
            wrapper = f"WITH {mode + ' ' if mode else ''}ARRAY WRAPPER"
            return exp.Var(this=wrapper)

        def _parse_ydb_json_query_on_clause(self, name: str) -> t.Optional[exp.Var]:
            action = None
            if self._match(TokenType.NULL):
                action = "NULL"
            elif self._curr and self._curr.text.upper() == "ERROR":
                self._advance()
                action = "ERROR"
            elif self._curr and self._curr.text.upper() == "EMPTY":
                self._advance()
                if self._match(TokenType.ARRAY):
                    action = "EMPTY ARRAY"
                elif self._match(TokenType.OBJECT):
                    action = "EMPTY OBJECT"
                else:
                    return None
            else:
                return None

            if not (self._match(TokenType.ON) and self._curr and self._curr.text.upper() == name):
                return None
            self._advance()
            return exp.Var(this=action)

        def _parse_unary(self) -> t.Optional[exp.Expression]:
            return self._parse_ydb_postfix_calls(super()._parse_unary())

        def _parse_type(
            self,
            parse_interval: bool = True,
            fallback_to_identifier: bool = False,
        ) -> t.Optional[exp.Expression]:
            if (
                self._curr
                and self._next
                and self._curr.token_type == TokenType.STRING
                and self._next.token_type == TokenType.VAR
                and self._next.text.lower() == "u"
                and self._curr.end + 1 == self._next.start
            ):
                token = self._curr
                suffix = self._next.text
                self._advance(2)
                literal = self.expression(exp.Literal(this=token.text, is_string=True), token)
                literal.meta["ydb_string_suffix"] = suffix
                return literal

            return super()._parse_type(
                parse_interval=parse_interval,
                fallback_to_identifier=fallback_to_identifier,
            )

        def _parse_ydb_postfix_calls(
            self,
            expression: t.Optional[exp.Expression],
        ) -> t.Optional[exp.Expression]:
            while expression is not None and self._match(TokenType.L_PAREN):
                args = self._parse_function_args(alias=True)
                self._match_r_paren(expression)
                expression = self.expression(YdbPostfixCall(this=expression, expressions=args))
            return expression

        def _parse_types(self, *args, **kwargs) -> t.Optional[exp.Expression]:
            # YDB generic types use Name<...> syntax; token type varies by keyword status
            if self._curr and self._next and self._next.token_type == TokenType.LT:
                name = self._curr.text

                if name == "Optional":
                    self._advance()  # consume 'Optional'
                    self._advance()  # consume '<'
                    inner = self._parse_types(*args, **kwargs)
                    self._match(TokenType.GT)
                    if inner:
                        inner.set("nullable", True)
                    return inner

                if name in _YDB_GENERIC_TYPES:
                    self._advance()  # consume type name
                    self._advance()  # consume '<'
                    type_args = self._parse_csv(
                        lambda: self._parse_types(*args, **kwargs)
                    )
                    self._match(TokenType.GT)
                    return exp.DataType(
                        this=_YDB_GENERIC_TYPES[name],
                        expressions=[a for a in type_args if a],
                        nested=True,
                    )

                if name == "Struct":
                    self._advance()  # consume 'Struct'
                    self._advance()  # consume '<'
                    fields = self._parse_csv(self._parse_ydb_struct_field)
                    self._match(TokenType.GT)
                    return exp.DataType(
                        this=exp.DataType.Type.STRUCT,
                        expressions=[field for field in fields if field],
                        nested=True,
                    )

                if name == "Tuple":
                    self._advance()  # consume 'Tuple'
                    self._advance()  # consume '<'
                    type_args = self._parse_csv(
                        lambda: self._parse_types(*args, **kwargs)
                    )
                    self._match(TokenType.GT)
                    # Represent as STRUCT so other dialects can serialize it.
                    # kind="tuple" is a YDB-specific marker for the generator to emit Tuple<...>.
                    return exp.DataType(
                        this=exp.DataType.Type.STRUCT,
                        expressions=[
                            exp.ColumnDef(this=exp.to_identifier(f"_{i}"), kind=a)
                            for i, a in enumerate(type_args) if a
                        ],
                        nested=True,
                        kind=exp.Var(this="tuple"),
                    )

            dtype = super()._parse_types(*args, **kwargs)
            if dtype and self._match(TokenType.PLACEHOLDER):  # T?
                dtype.set("nullable", True)
            return dtype

        def _parse_ydb_struct_field(self) -> t.Optional[exp.ColumnDef]:
            if self._curr and self._curr.token_type == TokenType.GT:
                return None

            field = self._parse_id_var(any_token=True)
            if not field:
                return None

            if not self._match(TokenType.COLON):
                self.raise_error("Expected colon after struct field name")

            kind = self._parse_types()
            return self.expression(exp.ColumnDef(this=field, kind=kind))

        def _parse_table_alias(self, alias_tokens=None):
            # Prevent YDB-specific keywords from being consumed as table aliases
            if self._curr and self._curr.text.upper() in ("FLATTEN", "ASSUME", "VIEW"):
                # Also check that what follows is a YDB construct, not a regular alias
                if self._next and (
                    self._next.text.upper() in ("BY", "LIST", "DICT")
                    or self._next.token_type == TokenType.ORDER_BY
                    or self._curr.text.upper() == "VIEW"
                ):
                    return None
            return super()._parse_table_alias(alias_tokens=alias_tokens)

        def _parse_query_modifiers(self, this):
            if (
                self._curr
                and self._curr.text.upper() == "ASSUME"
                and self._next
                and self._next.token_type == TokenType.ORDER_BY
            ):
                self._advance()  # consume ASSUME
                _, order = self.QUERY_MODIFIER_PARSERS[TokenType.ORDER_BY](self)
                if order and this:
                    this.set("order", self.expression(AssumeOrderBy(this=order)))
            return super()._parse_query_modifiers(this)

        def _parse_group(self, skip_group_by_token: bool = False) -> t.Optional[exp.Group]:
            # This mirrors sqlglot's Parser._parse_group control flow so YDB keeps
            # support for GROUP BY modifiers (WITH ROLLUP, CUBE, GROUPING SETS,
            # TOTALS). The YDB-specific change is parsing each group item through
            # _parse_alias(..., explicit=True), enabling `GROUP BY expr AS alias`.
            if not skip_group_by_token and not self._match(TokenType.GROUP_BY):
                return None
            comments = self._prev_comments

            elements: dict[str, t.Any] = defaultdict(list)

            if self._match(TokenType.ALL):
                elements["all"] = True
            elif self._match(TokenType.DISTINCT):
                elements["all"] = False

            if self._match_set(self.QUERY_MODIFIER_TOKENS, advance=False):
                return self.expression(exp.Group(**elements), comments=comments)

            def _parse_group_expression():
                if self._match_set((TokenType.CUBE, TokenType.ROLLUP), advance=False):
                    return None
                return self._parse_alias(self._parse_disjunction(), explicit=True)

            while True:
                index = self._index

                elements["expressions"].extend(self._parse_csv(_parse_group_expression))

                before_with_index = self._index
                with_prefix = self._match(TokenType.WITH)

                if cube_or_rollup := self._parse_cube_or_rollup(with_prefix=with_prefix):
                    key = "rollup" if isinstance(cube_or_rollup, exp.Rollup) else "cube"
                    elements[key].append(cube_or_rollup)
                elif grouping_sets := self._parse_grouping_sets():
                    elements["grouping_sets"].append(grouping_sets)
                elif self._match_text_seq("TOTALS"):
                    elements["totals"] = True

                if before_with_index <= self._index <= before_with_index + 1:
                    self._retreat(before_with_index)
                    break

                if index == self._index:
                    break

            return self.expression(exp.Group(**elements), comments=comments)

        def _parse_table(self, *args, **kwargs) -> t.Optional[exp.Expression]:
            if self._match(TokenType.L_BRACKET):
                parts = []
                while self._curr and not self._match(TokenType.R_BRACKET, advance=False):
                    parts.append(self._curr.text)
                    self._advance()
                self._match(TokenType.R_BRACKET)
                table = self.expression(exp.Table(this=exp.to_identifier("".join(parts))))
                table.set("alias", self._parse_table_alias())
                return table

            table = super()._parse_table(*args, **kwargs)
            if table and self._match(TokenType.VIEW):
                table.set("ydb_index_view", self._parse_id_var(any_token=True))
                table.set("alias", self._parse_table_alias())
            if table and self._curr and self._curr.text.upper() == "FLATTEN":
                self._advance()
                kind: t.Optional[str] = None
                if self._curr and self._curr.text.upper() in ("LIST", "DICT"):
                    kind = self._curr.text.upper()
                    self._advance()
                self._match_text_seq("BY")
                cols = self._parse_csv(self._parse_column)
                return self.expression(FlattenBy(this=table, expressions=cols, kind=kind))
            return table

        def _parse_struct_types(self, type_required=True) -> t.Optional[exp.Expression]:
            if not self._curr:
                return None

            key = self._parse_id_var()
            if not key:
                return None

            if not self._match(TokenType.COLON):
                self.raise_error("Expected colon after struct key")

            value = self._parse_conjunction()
            if not value:
                self.raise_error("Expected value after colon")

            return exp.EQ(this=key, expression=value)

        def _parse_primary(self) -> t.Optional[exp.Expression]:
            if (
                self._curr
                and self._curr.token_type == TokenType.PARAMETER
                and self._next
                and self._next.token_type == TokenType.PARAMETER
            ):
                return self._parse_at_raw_string()

            if self._match(TokenType.L_PAREN):
                comments = self._prev_comments
                if self._next_matching_rparen_is_arrow():
                    expressions = self._parse_csv(self._parse_lambda_arg)
                    self._match_r_paren()
                    self._match(TokenType.ARROW)
                    return self._parse_ydb_lambda(expressions)

                query = self._parse_select()

                if query:
                    expressions = [query]
                else:
                    expressions = self._parse_expressions()

                lambda_expr = self._parse_lambda_body(expressions)
                if lambda_expr:
                    return lambda_expr

                this = self._parse_query_modifiers(seq_get(expressions, 0))

                if not this and self._match(TokenType.R_PAREN, advance=False):
                    this = self.expression(exp.Tuple)
                elif isinstance(this, exp.UNWRAPPED_QUERIES):
                    this = self._parse_subquery(this=this, parse_alias=False)
                elif isinstance(this, exp.Subquery):
                    this = self._parse_subquery(
                        this=self._parse_set_operations(this), parse_alias=False
                    )
                elif len(expressions) > 1 or self._prev.token_type == TokenType.COMMA:
                    this = self.expression(exp.Tuple, expressions=expressions)
                else:
                    this = self.expression(exp.Paren, this=this)

                if this:
                    this.add_comments(comments)

                self._match_r_paren(expression=this)
                return this
            return super()._parse_primary()

        def _next_matching_rparen_is_arrow(self) -> bool:
            depth = 1
            # _tokens_size is not available in all supported sqlglot versions.
            for i in range(self._index, len(self._tokens)):
                token = self._tokens[i]
                if token.token_type == TokenType.L_PAREN:
                    depth += 1
                elif token.token_type == TokenType.R_PAREN:
                    depth -= 1
                    if depth == 0:
                        return (
                            i + 1 < len(self._tokens)
                            and self._tokens[i + 1].token_type == TokenType.ARROW
                        )
            return False

        def _parse_at_raw_string(self) -> YdbAtString:
            self._advance()
            self._advance()

            parts = []
            while self._curr:
                if (
                    self._curr.token_type == TokenType.PARAMETER
                    and self._next
                    and self._next.token_type == TokenType.PARAMETER
                ):
                    self._advance()
                    self._advance()
                    break

                parts.append(self._curr.text)
                self._advance()

            return self.expression(YdbAtString(this="".join(parts)))

        def _parse_lambda_body(self, params):
            if (
                    self._curr is None
                    or self._curr.token_type != TokenType.R_PAREN
                    or self._next is None
                    or self._next.token_type != TokenType.ARROW
            ):
                return None
            self._advance()
            self._advance()
            return self._parse_ydb_lambda(params)

        def _parse_ydb_lambda(self, params):
            has_brace = self._match(TokenType.L_BRACE)
            assignments = []

            if has_brace:
                while self._curr and self._curr.text != "RETURN":
                    assignment = self._parse_ydb_named_expr()
                    if not assignment:
                        self.raise_error("Expected lambda body expression after '->'")
                    assignments.append(assignment)
                    self._match(TokenType.SEMICOLON)

                if not self._match_text_seq("RETURN"):
                    self.raise_error("Expected lambda body RETURN after '->'")

            body = self._parse_disjunction()

            if not body:
                self.raise_error("Expected lambda body expression after '->'")

            self._match(TokenType.SEMICOLON)
            if has_brace:
                self._match(TokenType.R_BRACE, expression=body)
                if assignments:
                    body = self.expression(YdbLambdaBlock(this=body, expressions=assignments))

            return self.expression(exp.Lambda(this=body, expressions=params))

        def _parse_in(self, this: t.Optional[exp.Expression], alias: bool = False) -> exp.In:
            if self._match_text_seq("COMPACT"):
                expression = self.expression(exp.In(this=this, field=self._parse_column()))
                expression.meta["compact"] = True
                return expression
            return super()._parse_in(this, alias=alias)

    class Generator(generator.Generator):
        """
        SQL Generator for YDB dialect.
        Responsible for translating SQL AST back to SQL text with YDB-specific syntax.
        """

        PARAMETER_TOKEN = "$"

        SUPPORTS_VALUES_DEFAULT = False
        NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        NVL2_SUPPORTED = False
        JSON_PATH_BRACKETED_KEY_SUPPORTED = False
        SUPPORTS_CREATE_TABLE_LIKE = False
        SUPPORTS_TABLE_ALIAS_COLUMNS = False
        SUPPORTS_TO_NUMBER = False
        EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = False
        SUPPORTS_MEDIAN = False
        JSON_KEY_VALUE_PAIR_SEP = ","
        VARCHAR_REQUIRES_SIZE = False
        CAN_IMPLEMENT_ARRAY_ANY = True
        STRUCT_DELIMITER = ("<", ">")
        NULL_ORDERING_SUPPORTED: t.Optional[bool] = False
        NULL_ORDERING = None
        MATCHED_BY_SOURCE = False

        def __init__(self, **kwargs):
            """
            Initialize the YDB SQL Generator with optional configuration.

            Args:
                **kwargs: Additional keyword arguments to pass to the parent Generator.
            """
            super().__init__(**kwargs)
            self.expression_to_alias = {}
            self.ydb_variables = {}

        def create_sql(self, expression: exp.Create, pretty=True) -> str:
            """
            Generate SQL for CREATE expressions with special handling for CREATE VIEW.

            Args:
                expression: The CREATE expression to generate SQL for
                pretty: Whether to format the SQL with indentation

            Returns:
                Generated SQL string
            """
            if expression.kind == "VIEW" and expression.this and expression.this.this:
                ident = expression.this.this
                ident_sql = self.sql(ident)
                sql = self.sql(expression.expression)

                return f"CREATE VIEW {ident_sql} WITH (security_invoker = TRUE) AS {sql}"
            elif expression.kind == "FUNCTION":
                # CREATE -> FUNCTION -> TABLE
                func_name = self.sql(expression.this.this.alias_or_name)

                params = []
                for param in expression.this.expressions:
                    if isinstance(param, exp.ColumnDef):
                        param_name = self.sql(param.this)
                        params.append(f"${param_name}")
                    else:
                        params.append(self.sql(param))

                params_str = ", ".join(params)

                body = f" RETURN {self.sql(expression.expression)}"
                return f"${func_name} = ({params_str}) -> {{ {body} }};"
            else:
                return super().create_sql(expression)

        def table_sql(self, expression: exp.Table, copy=True) -> str:
            """
            Generate SQL for TABLE expressions with proper quoting and database prefix.

            Args:
                expression: The TABLE expression
                copy: Whether to copy the expression before processing

            Returns:
                Generated SQL string for the table reference
            """
            if isinstance(expression.this, exp.Parameter):
                var = self.sql(expression, "this")
                alias = f" AS {expression.alias}" if expression.alias else ""
                return f"{var}{alias}"
            if isinstance(expression.this, exp.Func):
                sql = self.sql(expression, "this")
                if expression.alias:
                    sql += f" AS {expression.alias}"
                return sql
            prefix = f"{expression.db}/" if expression.db else ""
            sql = f"`{prefix}{expression.name}`"

            ydb_index_view = self.sql(expression, "ydb_index_view")
            if ydb_index_view:
                sql += f" VIEW {ydb_index_view}"

            if expression.alias:
                sql += f" AS {expression.alias}"

            return sql

        def is_sql(self, expression: exp.Is) -> str:
            """
            Generate SQL for IS expressions with special handling for IS NOT NULL.

            Args:
                expression: The IS expression

            Returns:
                Generated SQL string
            """
            is_sql = super().is_sql(expression)

            if isinstance(expression.parent, exp.Not):
                # value IS NOT NULL -> NOT (value IS NULL)
                is_sql = self.wrap(is_sql)

            return is_sql

        def scoperesolution_sql(self, expression: exp.ScopeResolution) -> str:
            this = self.sql(expression, "this")
            expr = self.sql(expression, "expression")
            return f"{this}::{expr}"

        def literal_sql(self, expression: exp.Literal) -> str:
            sql = super().literal_sql(expression)
            suffix = expression.meta.get("ydb_string_suffix")
            return f"{sql}{suffix}" if suffix else sql

        def declareitem_sql(self, expression: exp.DeclareItem) -> str:
            name = self.sql(expression, "this")
            kind = self.sql(expression, "kind")
            return f"${name} AS {kind}"

        def flattenby_sql(self, expression: FlattenBy) -> str:
            table = self.sql(expression, "this")
            kind = expression.args.get("kind")
            kind_str = f" {kind}" if kind else ""
            cols = self.expressions(expression, flat=True)
            return f"{table} FLATTEN{kind_str} BY {cols}"

        def assumeorderby_sql(self, expression: AssumeOrderBy) -> str:
            order = self.sql(expression, "this").lstrip()
            return self.seg(f"ASSUME {order}")

        def ydbtuple_sql(self, expression: YdbTuple) -> str:
            inner = ", ".join(self.sql(e) for e in expression.expressions)
            sql = f"Tuple<{inner}>"
            return f"Optional<{sql}>" if expression.args.get("nullable") else sql

        def ydbatstring_sql(self, expression: YdbAtString) -> str:
            return f"@@{expression.this}@@"

        def ydbpostfixcall_sql(self, expression: YdbPostfixCall) -> str:
            this = self.sql(expression, "this")
            args = self.expressions(expression, flat=True)
            return f"{this}({args})"

        def ydbjsonvalue_sql(self, expression: YdbJsonValue) -> str:
            args = [self.sql(expression, "this"), self.sql(expression, "path")]
            passing = expression.args.get("passing") or []
            if passing:
                passing_sql = ", ".join(self.sql(item) for item in passing)
                args[-1] = f"{args[-1]} PASSING {passing_sql}"

            returning = self.sql(expression, "returning")
            if returning:
                args[-1] = f"{args[-1]} RETURNING {returning}"

            on_empty = self._ydb_json_on_clause_sql(expression.args.get("on_empty"), "EMPTY")
            if on_empty:
                args[-1] = f"{args[-1]} {on_empty}"

            on_error = self._ydb_json_on_clause_sql(expression.args.get("on_error"), "ERROR")
            if on_error:
                args[-1] = f"{args[-1]} {on_error}"

            return f"JSON_VALUE({', '.join(args)})"

        def ydbjsonexists_sql(self, expression: YdbJsonExists) -> str:
            args = [self.sql(expression, "this"), self.sql(expression, "path")]
            passing = expression.args.get("passing") or []
            if passing:
                passing_sql = ", ".join(self.sql(item) for item in passing)
                args[-1] = f"{args[-1]} PASSING {passing_sql}"

            on_error = expression.args.get("on_error")
            if on_error:
                args[-1] = f"{args[-1]} {on_error.name.upper()} ON ERROR"

            return f"JSON_EXISTS({', '.join(args)})"

        def ydbjsonquery_sql(self, expression: YdbJsonQuery) -> str:
            args = [self.sql(expression, "this"), self.sql(expression, "path")]
            passing = expression.args.get("passing") or []
            if passing:
                passing_sql = ", ".join(self.sql(item) for item in passing)
                args[-1] = f"{args[-1]} PASSING {passing_sql}"

            wrapper = expression.args.get("wrapper")
            if wrapper:
                args[-1] = f"{args[-1]} {wrapper.name.upper()}"

            on_empty = expression.args.get("on_empty")
            if on_empty:
                args[-1] = f"{args[-1]} {on_empty.name.upper()} ON EMPTY"

            on_error = expression.args.get("on_error")
            if on_error:
                args[-1] = f"{args[-1]} {on_error.name.upper()} ON ERROR"

            return f"JSON_QUERY({', '.join(args)})"

        def _ydb_json_on_clause_sql(
            self,
            expression: t.Optional[exp.Expression],
            name: str,
        ) -> str:
            if not expression:
                return ""
            action = expression.name.upper()
            if action == "DEFAULT":
                return f"DEFAULT {self.sql(expression, 'expression')} ON {name}"
            return f"{action} ON {name}"

        def ydbjsonpassingitem_sql(self, expression: YdbJsonPassingItem) -> str:
            alias = expression.args.get("alias") or ""
            if expression.args.get("quoted"):
                alias = f'"{alias}"'
            return f"{self.sql(expression, 'this')} AS {alias}"

        def ydblambdablock_sql(self, expression: YdbLambdaBlock) -> str:
            assignments = [self.sql(assignment) for assignment in expression.expressions]
            statements = [*assignments, f"RETURN {self.sql(expression, 'this')}"]
            return "{ " + "; ".join(statements) + " }"

        def in_sql(self, expression: exp.In) -> str:
            if expression.meta.get("compact"):
                return f"{self.sql(expression, 'this')} IN COMPACT {self.sql(expression, 'field')}"
            return super().in_sql(expression)

        def maybe_comment(
            self,
            sql: str,
            expression: t.Optional[exp.Expression] = None,
            comments: t.Optional[t.List[str]] = None,
            separated: bool = False,
        ) -> str:
            comments = (
                ((expression and expression.comments) if comments is None else comments)
                if self.comments
                else None
            )
            if not comments:
                return sql

            line_directives = [comment.strip() for comment in comments if comment.strip().startswith("!")]
            if not line_directives:
                return super().maybe_comment(sql, expression, comments=comments, separated=separated)

            remaining_comments = [comment for comment in comments if not comment.strip().startswith("!")]
            sql = super().maybe_comment(sql, expression, comments=remaining_comments, separated=separated)
            prefix = "\n".join(f"--{directive}" for directive in line_directives)
            return f"{prefix}\n{sql}" if sql else prefix

        def alias_sql(self, expression: exp.Alias) -> str:
            alias = expression.args.get("alias")
            if alias and alias.name.startswith("$"):
                return f"{alias.name} = {self.sql(expression, 'this')}"
            return super().alias_sql(expression)

        def anonymous_sql(self, expression: exp.Anonymous) -> str:
            """
            Generate SQL for Anonymous functions, with special handling for YQL lambda variables.
            Variables starting with $ should not be normalized.

            Args:
                expression: The Anonymous expression

            Returns:
                Generated SQL string
            """
            # We don't normalize qualified functions such as a.b.foo(), because they can be case-sensitive
            parent = expression.parent
            is_qualified = isinstance(parent, exp.Dot) and expression is parent.expression

            func_name = self.sql(expression, "this")
            # Don't normalize YQL lambda variables (starting with $) or qualified functions
            normalize = not (is_qualified or func_name.startswith("$"))
            return self.func(func_name, *expression.expressions, normalize=normalize)

        # YDB doesn't allow comparison of nullable and non-nullable types.
        # Wrapping it in a lambda can help circumvent this limitation.
        # def _wrap_non_optional(self, expr: exp.Expression) -> exp.Expression:
        #     """
        #     Helper to wrap non-Optional types using the YQL lambda function.
        #     Uses the $wrap_non_optional_in_comparisons lambda function.
        #
        #     Args:
        #         expr: The expression to potentially wrap
        #
        #     Returns:
        #         Expression wrapped using the lambda function
        #     """
        #     # Use the lambda function: $wrap_non_optional_in_comparisons(expr)
        #     return exp.Anonymous(this="$wrap_non_optional_in_comparisons", expressions=[expr])
        #
        # def eq_sql(self, expression: exp.EQ) -> str:
        #     """
        #     Generate SQL for EQ (equals) with Just() for non-Optional types.
        #     Wraps non-Optional values with Just() to make them Optional.
        #
        #     Args:
        #         expression: The EQ expression
        #
        #     Returns:
        #         Generated SQL string with Just() wrapping for non-Optional types
        #     """
        #     left = self._wrap_non_optional(expression.this)
        #     right = self._wrap_non_optional(expression.expression)
        #     return self.binary(exp.EQ(this=left, expression=right), "=")
        #
        # def neq_sql(self, expression: exp.NEQ) -> str:
        #     """
        #     Generate SQL for NEQ (not equals) with Just() for non-Optional types.
        #     Wraps non-Optional values with Just() to make them Optional.
        #
        #     Args:
        #         expression: The NEQ expression
        #
        #     Returns:
        #         Generated SQL string with Just() wrapping for non-Optional types
        #     """
        #     left = self._wrap_non_optional(expression.this)
        #     right = self._wrap_non_optional(expression.expression)
        #     return self.binary(exp.NEQ(this=left, expression=right), "<>")
        #
        # def gt_sql(self, expression: exp.GT) -> str:
        #     """
        #     Generate SQL for GT (greater than) with Just() for non-Optional types.
        #     Wraps non-Optional values with Just() to make them Optional.
        #
        #     Args:
        #         expression: The GT expression
        #
        #     Returns:
        #         Generated SQL string with Just() wrapping for non-Optional types
        #     """
        #     left = self._wrap_non_optional(expression.this)
        #     right = self._wrap_non_optional(expression.expression)
        #     return self.binary(exp.GT(this=left, expression=right), ">")
        #
        # def gte_sql(self, expression: exp.GTE) -> str:
        #     """
        #     Generate SQL for GTE (greater than or equal) with Just() for non-Optional types.
        #     Wraps non-Optional values with Just() to make them Optional.
        #
        #     Args:
        #         expression: The GTE expression
        #
        #     Returns:
        #         Generated SQL string with Just() wrapping for non-Optional types
        #     """
        #     left = self._wrap_non_optional(expression.this)
        #     right = self._wrap_non_optional(expression.expression)
        #     return self.binary(exp.GTE(this=left, expression=right), ">=")
        #
        # def lt_sql(self, expression: exp.LT) -> str:
        #     """
        #     Generate SQL for LT (less than) with Just() for non-Optional types.
        #     Wraps non-Optional values with Just() to make them Optional.
        #
        #     Args:
        #         expression: The LT expression
        #
        #     Returns:
        #         Generated SQL string with Just() wrapping for non-Optional types
        #     """
        #     left = self._wrap_non_optional(expression.this)
        #     right = self._wrap_non_optional(expression.expression)
        #     return self.binary(exp.LT(this=left, expression=right), "<")
        #
        # def lte_sql(self, expression: exp.LTE) -> str:
        #     """
        #     Generate SQL for LTE (less than or equal) with Just() for non-Optional types.
        #     Wraps non-Optional values with Just() to make them Optional.
        #
        #     Args:
        #         expression: The LTE expression
        #
        #     Returns:
        #         Generated SQL string with Just() wrapping for non-Optional types
        #     """
        #     left = self._wrap_non_optional(expression.this)
        #     right = self._wrap_non_optional(expression.expression)
        #     return self.binary(exp.LTE(this=left, expression=right), "<=")

        def datatype_sql(self, expression: exp.DataType) -> str:
            """
            Generate SQL for data type expressions with YDB-specific type mapping.

            Args:
                expression: The data type expression

            Returns:
                Generated SQL string for the data type
            """
            nullable = expression.args.get("nullable")

            def _struct_field_sql(field: exp.Expression) -> str:
                if isinstance(field, exp.Identifier) and field.args.get("quoted"):
                    name = field.name.replace("'", "\\'")
                    return f"'{name}'"
                return self.sql(field)

            # YDB generic container types rendered with <> syntax and correct casing
            if expression.args.get("nested"):
                type_value = expression.this
                # Tuple<...>: STRUCT with kind="tuple" marker
                if (
                    type_value == exp.DataType.Type.STRUCT
                    and isinstance(expression.args.get("kind"), exp.Var)
                    and expression.args["kind"].name == "tuple"
                ):
                    inner = ", ".join(
                        self.sql(col.args["kind"])
                        for col in expression.expressions
                        if isinstance(col, exp.ColumnDef)
                    )
                    sql = f"Tuple<{inner}>"
                    return f"Optional<{sql}>" if nullable else sql

                if type_value == exp.DataType.Type.STRUCT:
                    inner = ", ".join(
                        f"{_struct_field_sql(col.this)}: {self.sql(col, 'kind')}"
                        for col in expression.expressions
                        if isinstance(col, exp.ColumnDef)
                    )
                    sql = f"Struct<{inner}>"
                    return f"Optional<{sql}>" if nullable else sql

                inner = ", ".join(self.sql(e) for e in expression.expressions)
                name = {
                    exp.DataType.Type.LIST: "List",
                    exp.DataType.Type.MAP: "Dict",
                    exp.DataType.Type.SET: "Set",
                }.get(type_value)
                if name:
                    sql = f"{name}<{inner}>"
                    return f"Optional<{sql}>" if nullable else sql

            if (
                    expression.is_type(exp.DataType.Type.NVARCHAR)
                    or expression.is_type(exp.DataType.Type.VARCHAR)
                    or expression.is_type(exp.DataType.Type.CHAR)
            ):
                expression = exp.DataType.build("text")
            elif expression.is_type(exp.DataType.Type.DECIMAL):
                size_expressions = list(expression.find_all(exp.DataTypeParam))

                column_def = expression.parent
                is_pk = False
                if isinstance(column_def, exp.ColumnDef):
                    for constraint in column_def.constraints:
                        if isinstance(constraint.kind, exp.PrimaryKeyColumnConstraint):
                            expression = exp.DataType.build("int64")
                            is_pk = True

                if is_pk:
                    pass
                elif not size_expressions:
                    expression = exp.DataType.build("int64")
                else:
                    if len(size_expressions) == 1 or (
                            len(size_expressions) == 2 and int(size_expressions[1].name) == 0
                    ):
                        if isinstance(size_expressions[0].this, exp.Star):
                            expression = exp.DataType.build("decimal(38, 0)")
                        else:
                            mantis = int(size_expressions[0].name)
                            expression = exp.DataType.build(f"decimal({mantis}, 0)")
                    else:
                        precision = int(size_expressions[0].name)
                        scale = int(size_expressions[1].name)
                        expression = exp.DataType.build(f"decimal({precision}, {scale})")
            elif expression.is_type(exp.DataType.Type.TIMESTAMP):
                expression = exp.DataType.build("Timestamp")
            elif expression.this in exp.DataType.TEMPORAL_TYPES:
                expression = exp.DataType.build(expression.this)
            elif expression.is_type("float"):
                size_expression = expression.find(exp.DataTypeParam)
                if size_expression:
                    size = int(size_expression.name)
                    expression = (
                        exp.DataType.build("float") if size <= 32 else exp.DataType.build("double")
                    )

            sql = super().datatype_sql(expression)
            if nullable:
                sql = f"Optional<{sql}>"
            return sql

        def primarykeycolumnconstraint_sql(self, expression: exp.PrimaryKeyColumnConstraint) -> str:
            """
            Generate SQL for PRIMARY KEY column constraints.
            In YDB, these are handled differently at the table level.

            Args:
                expression: The PRIMARY KEY column constraint

            Returns:
                Empty string as YDB handles primary keys differently
            """
            return ""

        def _cte_to_lambda(self, expression: exp.Expression) -> str:
            """
            Convert Common Table Expressions (CTEs) to YDB-style lambdas.

            Args:
                expression: The SQL expression containing CTEs

            Returns:
                YDB-specific SQL with lambdas instead of CTEs
            """

            all_ctes = list(expression.find_all(exp.CTE))

            if not all_ctes:
                output = self.sql(expression)
            else:
                aliases = []

                def _table_to_var(node):
                    if (isinstance(node, exp.Table)) and node.name in aliases:
                        return exp.Var(this=f"${node.name} AS {node.alias_or_name}")
                    return node

                for cte in all_ctes:
                    alias = cte.alias
                    aliases.append(alias)

                expression.transform(_table_to_var, copy=False)

                for cte in all_ctes:
                    cte.pop()

                all_with = list(expression.find_all(exp.With))
                for w in all_with:
                    w.pop()

                output = ""

                for cte in all_ctes:
                    cte_body = cte.this.copy()
                    # Apply CTE column aliases (WITH name (col1, col2) AS (...)) to the
                    # SELECT expressions, because YDB's $var = (...) form doesn't support
                    # a column list and outer queries reference the aliased names.
                    cte_alias = cte.args.get("alias")
                    if isinstance(cte_alias, exp.TableAlias):
                        col_list = cte_alias.columns
                        if col_list and len(col_list) == len(cte_body.expressions):
                            new_exprs = []
                            for sel_expr, col_id in zip(cte_body.expressions, col_list):
                                alias_name = col_id.name if hasattr(col_id, "name") else str(col_id)
                                if not alias_name:
                                    new_exprs.append(sel_expr)
                                    continue
                                if isinstance(sel_expr, exp.Alias):
                                    new_sel = sel_expr.copy()
                                    new_sel.set("alias", exp.to_identifier(alias_name))
                                else:
                                    new_sel = exp.alias_(sel_expr.copy(), alias_name)
                                new_exprs.append(new_sel)
                            cte_body.set("expressions", new_exprs)
                    cte_sql = self.sql(cte_body)
                    output += f"${cte.alias_or_name} = ({cte_sql});\n\n"

                body_sql = self.sql(expression)

                output += body_sql

            ydb_vars_sql = ""
            for var_name, subquery in self.ydb_variables.items():
                subquery_sql = self.sql(subquery)
                ydb_vars_sql += f"${var_name} = ({subquery_sql});\n"
            self.ydb_variables = {}
            output = ydb_vars_sql + output
            return output

        def _generate_create_table(self, expression: exp.Expression) -> str:
            """
            Generate CREATE TABLE SQL with YDB-specific syntax.
            Handles primary keys, constraints, and partitioning.

            Args:
                expression: The CREATE TABLE expression

            Returns:
                SQL string for creating a table in YDB
            """
            # YDB has no CREATE OR REPLACE TABLE syntax — strip the OR REPLACE qualifier.
            # The caller is responsible for deciding whether to drop the table first.
            if expression.args.get("replace"):
                expression.set("replace", False)

            # Strip dialect-specific properties YDB doesn't understand (ENGINE, SETTINGS, …)
            props = expression.args.get("properties")
            if props:
                keep = [
                    p for p in props.expressions
                    if not isinstance(p, (exp.EngineProperty, exp.SettingsProperty))
                ]
                if keep:
                    props.set("expressions", keep)
                else:
                    expression.set("properties", None)

            # Clean up index parts from table
            for ex in list(expression.this.expressions):
                if isinstance(ex, exp.Identifier):
                    ex.pop()

            def enforce_not_null(col):
                """Add NOT NULL constraint if not present"""
                for constraint in col.constraints:
                    if isinstance(constraint.kind, exp.NotNullColumnConstraint):
                        break
                else:
                    col.append(
                        "constraints", exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())
                    )

            def enforce_pk(col):
                """Add PRIMARY KEY constraint if not present"""
                for constraint in col.constraints:
                    if isinstance(constraint.kind, exp.PrimaryKeyColumnConstraint):
                        break
                else:
                    col.append(
                        "constraints", exp.ColumnConstraint(kind=exp.PrimaryKeyColumnConstraint())
                    )

            pks = list(expression.find_all(exp.PrimaryKey))
            if len(pks) > 0:
                for pk in pks:
                    for pk_ex in pk.expressions:
                        pk_cols = [
                            col
                            for col in expression.this.find_all(exp.ColumnDef)
                            if col.alias_or_name.lower() == pk_ex.alias_or_name.lower()
                        ]
                        if len(pk_cols) > 0:
                            col = pk_cols[0]
                            enforce_not_null(col)
                            enforce_pk(col)
                    pk.pop()

            def is_pk(col):
                """Check if a column has a PRIMARY KEY constraint"""
                for constraint in col.constraints:
                    if isinstance(constraint, exp.ColumnConstraint):
                        if isinstance(constraint.kind, exp.PrimaryKeyColumnConstraint):
                            return True
                return False

            for col in expression.find_all(exp.ColumnDef):
                if is_pk(col):
                    break
            else:
                col = list(expression.find_all(exp.ColumnDef))[0]
                enforce_pk(col)

            for col in expression.this.find_all(exp.ColumnDef):
                if is_pk(col):
                    enforce_not_null(col)

            for constraint in list(expression.this.find_all(exp.Constraint)):
                constraint.pop()

            sql = super().generate(expression)

            pk_s = []
            for col in expression.find_all(exp.ColumnDef):
                if is_pk(col):
                    pk_s.append(col.alias_or_name)

            if not pk_s:
                raise ValueError("No primary key columns found")
            ind = sql.rfind(")")
            col_names = ",".join([f"`{pk}`" for pk in pk_s])
            sql = sql[:ind] + f", PRIMARY KEY({col_names}))\nPARTITION BY HASH ({col_names});"
            return sql

        def generate(self, expression: exp.Expression, copy: bool = True) -> str:
            """
            Generate SQL for any expression with YDB-specific handling.

            Args:
                expression: The SQL expression to generate
                copy: Whether to copy the expression before processing

            Returns:
                Generated SQL string
            """

            self.unnest_subqueries(expression)
            expression = eliminate_join_marks(expression)
            expression = expression.copy() if copy else expression
            _simplify_double_not(expression)
            _apply_subquery_alias_columns(expression)
            _cast_date_string_literals(expression)
            _alias_order_by_aggregates(expression)
            _expand_positional_group_by(expression)
            _wrap_udf_group_by(expression)

            if not isinstance(expression, exp.Create) or (
                    isinstance(expression, exp.Create)
                    and expression.kind
                    and expression.kind.lower() != "table"
            ):
                sql = self._cte_to_lambda(expression)
            else:
                sql = self._generate_create_table(expression)

            return sql

        def unnest_subqueries(self, expression):
            """
            Rewrite sqlglot AST to convert some predicates with subqueries into joins.

            Convert scalar subqueries into cross joins.
            Convert correlated or vectorized subqueries into a group by so it is not a many to many left join.

            Example:
                >>> import sqlglot
                >>> expression = sqlglot.parse_one("SELECT * FROM x AS x WHERE (SELECT y.a AS a FROM y AS y WHERE x.a = y.a) = 1 ")
                >>> unnest_subqueries(expression).sql()
                'SELECT * FROM x AS x LEFT JOIN (SELECT y.a AS a FROM y AS y WHERE TRUE GROUP BY y.a) AS _u_0 ON x.a = _u_0.a WHERE _u_0.a = 1'

            Args:
                expression (sqlglot.Expression): expression to unnest
            Returns:
                sqlglot.Expression: unnested expression
            """
            next_alias_name = name_sequence("_u_")

            for scope in traverse_scope(expression):
                select = scope.expression
                parent = select.parent_select
                if not parent:
                    if scope.external_columns:
                        # Correlated subquery inside a DML statement (UPDATE/INSERT).
                        # YDB does not support correlated subqueries in DML context and
                        # automatic decorrelation via JOIN is not possible without knowing
                        # the table's primary key. Rewrite manually using a $variable, e.g.:
                        #   $rows = (SELECT id FROM t WHERE <condition>);
                        #   UPDATE t SET ... WHERE id IN (SELECT id FROM $rows);
                        dml = select.find_ancestor(exp.Update, exp.Insert)
                        if dml is not None:
                            kind = type(dml).__name__.upper()
                            raise UnsupportedError(
                                f"Correlated subquery inside {kind} cannot be automatically "
                                f"decorrelated in YDB — rewrite manually using a $variable subquery"
                            )
                    continue
                if scope.external_columns and scope.scope_type != ScopeType.CTE:
                    self.decorrelate(select, parent, scope.external_columns, next_alias_name)
                if scope.scope_type == ScopeType.SUBQUERY:
                    self.unnest(select, parent, next_alias_name)

            return expression

        @staticmethod
        def remove_star_when_other_columns(expression: exp.Expression) -> exp.Expression:
            """
            Remove * from SELECT list when there are other columns present.

            Args:
                expression: The SQL expression to modify

            Returns:
                Modified expression without redundant *
            """
            for select_expr in expression.find_all(exp.Select):
                expressions = select_expr.expressions

                # Check if there's a * and at least one other column
                has_star = any(
                    isinstance(expr, exp.Star)
                    or (isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star))
                    for expr in expressions
                )

                has_other_columns = any(
                    not (
                            isinstance(expr, exp.Star)
                            or (isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star))
                    )
                    for expr in expressions
                )

                if has_star and has_other_columns:
                    # Remove all * expressions
                    new_expressions = [
                        expr
                        for expr in expressions
                        if not (
                                isinstance(expr, exp.Star)
                                or (isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star))
                        )
                    ]
                    select_expr.set("expressions", new_expressions)

            return expression

        def unnest(self, select, parent_select, next_alias_name):
            """
            Unnests a subquery by transforming it into a join
            """
            if isinstance(select.parent, exp.CTE):
                return

            if len(select.selects) > 1:
                return
            self.ensure_select_aliases(select)

            predicate = select.find_ancestor(exp.Condition)
            if (
                    not predicate
                    or parent_select is not predicate.parent_select
                    or not parent_select.args.get("from_")
            ):
                return

            if any(
                    isinstance(expr, exp.Star)
                    or (isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star))
                    for expr in select.selects
            ):
                return

            if isinstance(select, exp.SetOperation):
                select = exp.select(*select.selects).from_(select.subquery(next_alias_name()))

            alias = next_alias_name()
            clause = predicate.find_ancestor(exp.Having, exp.Where, exp.Join)

            # This subquery returns a scalar and can just be converted to a cross join
            if not isinstance(predicate, (exp.In, exp.Any)):
                first_select = select.selects[0]
                column_alias = first_select.alias_or_name

                if (
                        not column_alias
                        or column_alias == ""
                        or (column_alias == "*" and isinstance(first_select, exp.AggFunc))
                ):
                    if isinstance(first_select, exp.Alias):
                        expr = first_select.this
                    else:
                        expr = first_select

                    # Generate a meaningful alias based on the expression type
                    if isinstance(expr, exp.AggFunc):
                        func_name = expr.sql_name().lower() if hasattr(expr, "sql_name") else "agg"
                        column_alias = f"_{func_name}"
                    else:
                        column_alias = "_col"

                    # Add alias to the select if it doesn't have one
                    if not isinstance(first_select, exp.Alias):
                        new_selects = [exp.alias_(first_select.copy(), column_alias)]
                        if len(select.selects) > 1:
                            new_selects.extend(select.selects[1:])
                        select.set("expressions", new_selects)
                        # Update first_select to point to the newly aliased expression
                        first_select = select.selects[0]
                    elif not first_select.alias or first_select.alias_or_name == "*":
                        first_select.set("alias", exp.to_identifier(column_alias))

                    # Re-read the alias after setting it to ensure we have the correct value
                    column_alias = first_select.alias_or_name

                column = exp.column(column_alias, alias)

                clause_parent_select = clause.parent_select if clause else None

                if (isinstance(clause, exp.Having) and clause_parent_select is parent_select) or (
                        (not clause or clause_parent_select is not parent_select)
                        and (
                                parent_select.args.get("group")
                                or any(
                            find_in_scope(select, exp.AggFunc) for select in parent_select.selects
                        )
                        )
                ):
                    column = exp.Max(this=column)
                elif not isinstance(select.parent, exp.Subquery) and not isinstance(
                        select.parent, exp.Exists
                ):
                    return

                _replace(select.parent, column)
                parent_select.join(select, join_type="CROSS", join_alias=alias, copy=False)
                return

            if select.find(exp.Limit, exp.Offset):
                return

            # YDB supports NOT IN (SELECT ...) natively for non-correlated subqueries.
            # Unnesting NOT IN produces an unqualified column in the JOIN ON clause which
            # YDB rejects with "JOIN: column requires correlation name".  Since unnest()
            # is only called for non-correlated subqueries (correlated ones go through
            # decorrelate()), we can safely pass NOT IN through to YDB unchanged.
            if isinstance(predicate, exp.In) and isinstance(predicate.parent, exp.Not):
                return

            if isinstance(predicate, exp.Any):
                predicate = predicate.find_ancestor(exp.EQ)

                if not predicate or parent_select is not predicate.parent_select:
                    return

            column = _other_operand(predicate)
            self.ensure_select_aliases(select)
            value = select.selects[0]
            join_key = exp.column(value.alias, alias)
            join_key_not_null = join_key.is_(exp.null()).not_()

            if isinstance(clause, exp.Join):
                _replace(predicate, exp.true())
                parent_select.where(join_key_not_null, copy=False)
            else:
                _replace(predicate, join_key_not_null)

            group = select.args.get("group")

            if group:
                # Remove table qualifiers from GROUP BY expressions
                group_expressions = []
                for expr in group.expressions:
                    if isinstance(expr, exp.Column) and expr.table:
                        # Remove table qualifier
                        unqualified_expr = exp.Column(this=expr.this)
                        group_expressions.append(unqualified_expr)
                    else:
                        group_expressions.append(expr)

                # Check if value.this (without qualifier) matches any group expression
                value_this_unqualified = value.this
                if isinstance(value_this_unqualified, exp.Column) and value_this_unqualified.table:
                    value_this_unqualified = exp.Column(this=value_this_unqualified.this)

                if {value_this_unqualified} != set(group_expressions):
                    select = (
                        exp.select(exp.alias_(exp.column(value.alias, "_q"), value.alias))
                        .from_(select.subquery("_q", copy=False), copy=False)
                        .group_by(exp.column(value.alias, "_q"), copy=False)
                    )
                else:
                    # Update group with unqualified expressions
                    new_group = exp.Group(expressions=group_expressions)
                    select.set("group", new_group)
            elif not find_in_scope(value.this, exp.AggFunc):
                # Remove table qualifier from value.this if it's a column for GROUP BY
                group_by_expr = value.this
                if isinstance(group_by_expr, exp.Column) and group_by_expr.table:
                    group_by_expr = exp.Column(this=group_by_expr.this)
                select = select.group_by(group_by_expr, copy=False)

            parent_select.join(
                select,
                on=column.eq(join_key),
                join_type="LEFT",
                join_alias=alias,
                copy=False,
            )

        @staticmethod
        def ensure_select_aliases(select, default_prefix="_col"):
            """
            Ensure all select expressions have a non-empty, unique alias.
            Use the original column name as alias if possible.
            """
            for i, expr in enumerate(select.selects):
                if isinstance(expr, exp.Alias):
                    alias_name = expr.alias_or_name
                    if not alias_name or alias_name == "*":
                        base_name = (
                            expr.this.alias_or_name
                            if hasattr(expr.this, "alias_or_name")
                            else f"{default_prefix}{i}"
                        )
                        expr.set("alias", exp.to_identifier(base_name))
                elif isinstance(expr, exp.Column):
                    base_name = expr.alias_or_name or f"{default_prefix}{i}"
                    select.selects[i] = exp.alias_(expr, base_name)
                else:
                    select.selects[i] = exp.alias_(expr, f"{default_prefix}{i}")

        def decorrelate(self, select, parent_select, external_columns, next_alias_name):
            """
            Decorrelates a subquery by transforming it into a join
            """
            where = select.args.get("where")
            if not where or where.find(exp.Or) or select.find(exp.Limit, exp.Offset):
                return

            table_alias = next_alias_name()
            keys = []

            # for all external columns in the where statement, find the relevant predicate
            # keys to convert it into a join
            for column in external_columns:
                predicate = column.find_ancestor(exp.Predicate)

                if isinstance(predicate, exp.Binary):
                    key = (
                        predicate.right
                        if any(node is column for node in predicate.left.walk())
                        else predicate.left
                    )
                elif isinstance(predicate, exp.Between):
                    key = predicate.this
                else:
                    return

                keys.append((key, column, predicate))

            is_subquery_projection = any(
                node is select.parent
                for node in map(lambda s: s.unalias(), parent_select.selects)
                if isinstance(node, exp.Subquery)
            )

            value = select.selects[0]
            key_aliases = {}
            group_by = []

            external_tables = [
                col.table
                for col in external_columns
                if isinstance(col, exp.Column) and hasattr(col, "table") and col.table
            ]

            external_column_set = set()
            for col in external_columns:
                if isinstance(col, exp.Column):
                    if col.table:
                        external_column_set.add(
                            (
                                col.table,
                                col.this.name if hasattr(col.this, "name") else col.alias_or_name,
                            )
                        )

            def is_external_column(col):
                if not isinstance(col, exp.Column):
                    return False
                col_table = col.table if col.table else None
                col_name = col.this.name if hasattr(col.this, "name") else col.alias_or_name
                return (col_table, col_name) in external_column_set or (
                    None,
                    col_name,
                ) in external_column_set

            keys = [
                (key, column, predicate)
                for key, column, predicate in keys
                if isinstance(key, exp.Column)
                   and (
                           not key.table  # No table qualifier = from subquery
                           or (
                                   key.table and key.table not in external_tables
                           )  # Has qualifier but not external
                   )
                   and is_external_column(column)
            ]  # Verify column is actually external

            parent_predicate = select.find_ancestor(exp.Predicate)
            is_exists = isinstance(parent_predicate, exp.Exists)

            if is_exists and not keys:
                return

            if is_exists:
                select.set("expressions", [])

            for key, _, predicate in keys:
                if is_exists:
                    if key not in key_aliases:
                        alias_name = next_alias_name()
                        key_aliases[key] = alias_name

                        key_copy = key.copy()
                        if isinstance(key_copy, exp.Column) and key_copy.table:
                            key_copy.set("table", None)

                        select.select(exp.alias_(key_copy, alias_name, quoted=False), copy=False)

                    if isinstance(predicate, exp.EQ) and key not in group_by:
                        group_by.append(key)
                else:
                    if value and key == value.this:
                        alias = value.alias if value.alias != "" else next_alias_name()
                        key_aliases[key] = alias
                        group_by.append(key)
                    else:
                        key_aliases[key] = next_alias_name()
                        if isinstance(predicate, exp.EQ) and key not in group_by:
                            group_by.append(key)

            if is_exists:
                value_alias = "_exists_flag"
                select.select(
                    exp.alias_(exp.Literal.number(1), value_alias, quoted=False), copy=False
                )
                alias = exp.column(value_alias, table_alias)
            elif value:
                agg_func = exp.Max if is_subquery_projection else exp.ArrayAgg

                # exists queries should not have any selects as it only checks if there are any rows
                # all selects will be added by the optimizer and only used for join keys
                for key, alias_val in key_aliases.items():
                    if key in group_by:
                        # add all keys to the projections of the subquery
                        # so that we can use it as a join keyjoin_sql
                        select.select(exp.alias_(key.copy(), alias_val, quoted=False), copy=False)
                    else:
                        select.select(
                            exp.alias_(agg_func(this=key.copy()), alias_val, quoted=False),
                            copy=False,
                        )

                if not value.alias_or_name or value.alias_or_name == "*":
                    # Generate a meaningful alias based on the expression type
                    if isinstance(value.this, exp.Count):
                        value_alias = "_count"
                    elif isinstance(value.this, exp.AggFunc):
                        func_name = (
                            value.this.sql_name().lower()
                            if hasattr(value.this, "sql_name")
                            else "agg"
                        )
                        value_alias = f"_{func_name}"
                    else:
                        value_alias = next_alias_name()

                    if isinstance(value, exp.Alias):
                        value.set("alias", value_alias)
                    else:
                        value = exp.alias_(value, value_alias)
                        select.selects[0] = value
                else:
                    value_alias = value.alias_or_name
                alias = exp.column(value_alias, table_alias)
            else:
                return

            self.remove_star_when_other_columns(select)
            other = _other_operand(parent_predicate)
            op_type = type(parent_predicate.parent) if parent_predicate else None

            if is_exists:
                if key_aliases:
                    first_key_alias = list(key_aliases.values())[0]
                    alias = exp.column(first_key_alias, table_alias)
                    parent_predicate.replace(exp.condition(f"NOT {self.sql(alias)} IS NULL"))
                else:
                    if select.selects:
                        first_select = select.selects[0]
                        alias_name = first_select.alias_or_name or "_exists"
                        alias = exp.column(alias_name, table_alias)
                        parent_predicate.replace(exp.condition(f"NOT {self.sql(alias)} IS NULL"))
            elif isinstance(parent_predicate, exp.All):
                if not issubclass(op_type, exp.Binary):
                    raise ValueError("op_type must be a subclass of Binary")
                assert issubclass(op_type, exp.Binary)
                predicate = op_type(this=other, expression=exp.column("_x"))
                _replace(parent_predicate.parent, f"ARRAY_ALL({alias}, _x -> {predicate})")
            elif isinstance(parent_predicate, exp.Any):
                if not issubclass(op_type, exp.Binary):
                    raise ValueError("op_type must be a subclass of Binary")
                if value.this in group_by:
                    predicate = op_type(this=other, expression=alias)
                    _replace(parent_predicate.parent, predicate)
                else:
                    predicate = op_type(this=other, expression=exp.column("_x"))
                    _replace(parent_predicate, f"ARRAY_ANY({alias}, _x -> {predicate})")
            elif isinstance(parent_predicate, exp.In):
                if value.this in group_by:
                    _replace(parent_predicate, f"{other} = {alias}")
                else:
                    _replace(
                        parent_predicate,
                        f"ARRAY_ANY({alias}, _x -> _x = {parent_predicate.this})",
                    )
            else:
                if is_subquery_projection and select.parent.alias:
                    alias = exp.alias_(alias, select.parent.alias)

                # COUNT always returns 0 on empty datasets, so we need take that into consideration here
                # by transforming all counts into 0 and using that as the coalesced value
                # However, don't add COALESCE if value.this is a Star (from COUNT(*)) -
                # scalar subqueries are handled by unnest which creates proper aliases
                if value.find(exp.Count) and not isinstance(value.this, exp.Star):

                    def remove_aggs(node):
                        if isinstance(node, exp.Count):
                            return exp.Literal.number(0)
                        elif isinstance(node, exp.AggFunc):
                            return exp.null()
                        return node

                    transformed = value.this.transform(remove_aggs)
                    # Only add COALESCE if the transformed expression is not a Star
                    if not isinstance(transformed, exp.Star):
                        alias = exp.Coalesce(this=alias, expressions=[transformed])

                select.parent.replace(alias)

            on_predicates = []

            for key, column, predicate in keys:
                if isinstance(predicate, exp.EQ):
                    predicate.replace(exp.true())

                    # Create the ON condition: external_column = subquery_alias.column_alias
                    if key in key_aliases:
                        # Use the alias we created for the key in the SELECT list
                        nested_col = exp.column(key_aliases[key], table_alias)

                        external_col_copy = column.copy()

                        on_predicates.append(exp.EQ(this=external_col_copy, expression=nested_col))
                else:
                    if key in key_aliases:
                        nested_col = exp.column(key_aliases[key], table_alias)

                        key.replace(nested_col)

            if group_by:
                new_group_by = []
                for gb_expr in group_by:
                    if isinstance(gb_expr, exp.Column) and gb_expr.table:
                        unqualified_expr = exp.Column(this=gb_expr.this)
                        new_group_by.append(unqualified_expr)
                    else:
                        new_group_by.append(gb_expr)
                group_by = new_group_by

            if on_predicates:
                if len(on_predicates) == 1:
                    on_clause = on_predicates[0]
                else:
                    on_clause = on_predicates[0]
                    for pred in on_predicates[1:]:
                        on_clause = exp.and_(on_clause, pred)

                parent_select.join(
                    select.group_by(*group_by, copy=False) if group_by else select,
                    on=on_clause,
                    join_type="LEFT",
                    join_alias=table_alias,
                    copy=False,
                )
            else:
                parent_select.join(
                    select.group_by(*group_by, copy=False) if group_by else select,
                    join_type="CROSS",
                    join_alias=table_alias,
                    copy=False,
                )

        STRING_TYPE_MAPPING = {
            exp.DataType.Type.BLOB: "String",
            exp.DataType.Type.CHAR: "String",
            exp.DataType.Type.LONGBLOB: "String",
            exp.DataType.Type.LONGTEXT: "String",
            exp.DataType.Type.MEDIUMBLOB: "String",
            exp.DataType.Type.MEDIUMTEXT: "String",
            exp.DataType.Type.TINYBLOB: "String",
            exp.DataType.Type.TINYTEXT: "String",
            exp.DataType.Type.TEXT: "Utf8",
            exp.DataType.Type.VARBINARY: "String",
            exp.DataType.Type.VARCHAR: "Utf8",
        }

        def _date_trunc_sql(self, expression: exp.DateTrunc) -> str:
            """
            Generate SQL for DATE_TRUNC function with YDB-specific implementation.

            Args:
                expression: The DATE_TRUNC expression

            Returns:
                YDB-specific SQL for truncating dates
            """
            expr = self.sql(expression, "this")
            unit = expression.text("unit").upper()

            if unit == "YEAR":
                return f"DateTime::MakeDate(DateTime::StartOfYear({expr}))"
            elif unit == "QUARTER":
                return f"DateTime::MakeDate(DateTime::StartOfQuarter({expr}))"
            elif unit == "MONTH":
                return f"DateTime::MakeDate(DateTime::StartOfMonth({expr}))"
            elif unit == "WEEK":
                return f"DateTime::MakeDate(DateTime::StartOfWeek({expr}))"
            elif unit == "DAY":
                return self.func("DATE", expr)
            elif unit == "HOUR":
                # Truncate to hour: subtract the minute and second components
                return (
                    f"({expr}"
                    f" - DateTime::IntervalFromMinutes(CAST(DateTime::GetMinute({expr}) AS Int32))"
                    f" - DateTime::IntervalFromSeconds(CAST(DateTime::GetSecond({expr}) AS Int32)))"
                )
            elif unit == "MINUTE":
                # Truncate to minute: subtract the second component
                return (
                    f"({expr}"
                    f" - DateTime::IntervalFromSeconds(CAST(DateTime::GetSecond({expr}) AS Int32)))"
                )
            else:
                self.unsupported(f"Unexpected DATE_TRUNC unit: {unit}")
                return self.func("DATE", expr)

        def _current_timestamp_sql(self, expression: exp.CurrentTimestamp) -> str:
            """
            Generate SQL for CURRENT_TIMESTAMP function with YDB-specific implementation.

            Args:
                expression: The CURRENT_TIMESTAMP expression

            Returns:
                YDB-specific SQL for current timestamp
            """
            return "CurrentUtcTimestamp()"

        def _str_to_date(self, expression: exp.StrToDate) -> str:
            """
            Generate SQL for STR_TO_DATE function with YDB-specific implementation.

            Args:
                expression: The STR_TO_DATE expression

            Returns:
                YDB-specific SQL for converting strings to dates
            """
            str_value = expression.this.name
            # formatted_time = self.format_time(expression, self.dialect.INVERSE_FORMAT_MAPPING,
            #                                   self.dialect.INVERSE_FORMAT_TRIE)
            formatted_time = self.format_time(expression)
            return f'DateTime::MakeTimestamp(DateTime::Parse({formatted_time})("{str_value}"))'

        def _extract(self, expression: exp.Extract) -> str:
            """
            Generate SQL for EXTRACT function with YDB-specific implementation.

            Args:
                expression: The EXTRACT expression

            Returns:
                YDB-specific SQL for extracting date parts
            """
            unit = expression.name.upper()
            expr = self.sql(expression.expression)

            _EXTRACT_MAP = {
                "YEAR": "DateTime::GetYear",
                "MONTH": "DateTime::GetMonth",
                "WEEK": "DateTime::GetWeekOfYear",
                "DAY": "DateTime::GetDayOfMonth",
                "HOUR": "DateTime::GetHour",
                "MINUTE": "DateTime::GetMinute",
                "SECOND": "DateTime::GetSecond",
            }
            if unit in _EXTRACT_MAP:
                return f"{_EXTRACT_MAP[unit]}({expr})"
            self.unsupported(f"Unexpected EXTRACT unit: {unit}")
            return self.func("DATE", expr)

        def _lambda(self, expression: exp.Lambda, arrow_sep: str = "->") -> str:
            """
            Generate SQL for Lambda expressions with YDB-specific syntax.

            Args:
                expression: The Lambda expression
                arrow_sep: The separator to use between parameters and body

            Returns:
                YDB-specific SQL for lambda functions
            """
            def _arg_name(arg: exp.Expression) -> str:
                if isinstance(arg, exp.Parameter):
                    return arg.name
                return arg.name if hasattr(arg, "name") else self.sql(arg).lstrip("$")

            def _arg_sql(arg: exp.Expression) -> str:
                name = _arg_name(arg)
                sql = f"${name}" if name and not name.startswith("$") else self.sql(arg)
                return f"{sql}?" if arg.meta.get("optional") else sql

            def _prefix_lambda_refs(node: exp.Expression) -> exp.Expression:
                if (
                    isinstance(node, exp.Identifier)
                    and not node.name.startswith("$")
                ):
                    return exp.Identifier(this=f"${node.name}", quoted=False)
                return node

            args = ", ".join(_arg_sql(arg) for arg in expression.expressions)
            body = expression.this.copy().transform(_prefix_lambda_refs)
            if isinstance(body, YdbLambdaBlock):
                return f"({args}) {arrow_sep} {self.sql(body)}"
            body_sql = self.sql(body)
            return f"({args}) {arrow_sep} {body_sql if isinstance(body, exp.Paren) else f'({body_sql})'}"

        def _is_simple_expression(self, expr: exp.Expression) -> bool:
            """
            Check if an expression is simple enough to be used directly in CASE/IF.
            Simple expressions are literals, columns, identifiers, and basic operations.

            Args:
                expr: The expression to check

            Returns:
                True if the expression is simple, False otherwise
            """
            if isinstance(expr, (exp.Literal, exp.Null)):
                return True

            if isinstance(expr, exp.Column):
                col_name = (
                    expr.this.name
                    if hasattr(expr.this, "name")
                    else (expr.alias_or_name if hasattr(expr, "alias_or_name") else None)
                )
                if not col_name or col_name == "*" or col_name == "":
                    return False
                return True

            if isinstance(expr, (exp.Star, exp.Identifier)):
                return True

            if isinstance(expr, exp.Binary):
                return self._is_simple_expression(expr.this) and self._is_simple_expression(
                    expr.expression
                )
            if isinstance(expr, exp.Paren):
                return self._is_simple_expression(expr.this)
            if isinstance(expr, (exp.Subquery, exp.Case, exp.If, exp.Func, exp.AggFunc)):
                return False
            return not any(
                isinstance(node, (exp.Subquery, exp.Case, exp.If, exp.Func, exp.AggFunc))
                for node in expr.walk()
                if node is not expr
            )

        def _references_unnest_alias(self, expr: exp.Expression) -> bool:
            """
            Check if an expression references table aliases from unnesting (like _u_0, _u_1).
            These aliases are only available in the main query, not in standalone SELECT statements.

            Args:
                expr: The expression to check

            Returns:
                True if the expression references an unnest alias, False otherwise
            """
            for node in expr.walk():
                if isinstance(node, exp.Column) and hasattr(node, "table") and node.table:
                    table_name = (
                        node.table
                        if isinstance(node.table, str)
                        else (node.table.name if hasattr(node.table, "name") else str(node.table))
                    )
                    if table_name and table_name.startswith("_u_"):
                        return True
            return False

        def _if(self, expression: exp.If) -> str:
            # Extract complex expressions to variables
            condition = expression.this
            true_expr = expression.args.get("true")
            false_expr = expression.args.get("false")


            condition = condition.copy()
            true_expr = true_expr.copy()
            false_expr = false_expr.copy()

            this = self.sql(condition)
            true = self.sql(true_expr) if true_expr else ""
            false = self.sql(false_expr) if false_expr else ""
            return f"IF({this}, {true}, {false})"

        def round_sql(self, expression: exp.Round) -> str:
            # SQL ROUND(x, n) rounds to n decimal places (positive = fractional digits).
            # YDB Math::Round(x, n) uses the opposite sign convention: n is the power of 10
            # to round to, so n=2 means "round to nearest 100", n=-2 means "2 decimal places".
            # We negate the precision argument to match SQL semantics.
            this = self.sql(expression, "this")
            decimals = expression.args.get("decimals")
            if decimals is None:
                return f"Math::Round({this})"
            negated = exp.Neg(this=decimals) if not isinstance(decimals, exp.Neg) else decimals.this
            return f"Math::Round({this}, {self.sql(negated)})"

        def count_sql(self, expression: exp.Count) -> str:
            # ClickHouse count() (no args) → COUNT(*) in YQL
            if not expression.this and not expression.expressions:
                return "COUNT(*)"
            return self.function_fallback_sql(expression)

        def _null_if(self, expression: exp.Nullif) -> str:
            lhs = expression.this
            rhs = expression.expression

            cond = exp.EQ(this=lhs, expression=rhs)
            return self.sql(exp.If(this=cond, true=exp.Null(), false=lhs))

        E = t.TypeVar("E", bound=Expression)

        def _simplify_unless_literal(self, expression: E) -> E:
            if not isinstance(expression, exp.Literal):
                expression = simplify(expression, dialect=self.dialect)
            return expression

        # we move the WHERE expression from ON, using literals
        def join_sql(self, expression: exp.Join) -> str:
            on_condition = expression.args.get("on")
            using = expression.args.get("using")

            # Any join with no ON/USING clause becomes an explicit CROSS JOIN.
            # YDB requires an ON clause for outer joins, and emitting CROSS JOIN
            # explicitly (instead of the comma-separated form) keeps the output
            # valid without any extra pragma.
            if not on_condition and not using:
                expression.set("kind", "CROSS")
                expression.set("side", None)
                return super().join_sql(expression)

            if on_condition:
                # For OUTER JOINs (LEFT / RIGHT / FULL), keep the entire ON clause intact.
                # Moving non-equality conditions from ON to WHERE changes the semantics:
                # in a LEFT JOIN, a non-equality filter in ON still produces a row for the
                # left-side record (with NULLs on the right), whereas the same filter in
                # WHERE would eliminate that row.  Pass outer-join ON clauses through
                # unchanged; YDB accepts non-equality predicates in OUTER JOIN ON.
                join_is_outer = any(
                    k in (expression.side or "").upper()
                    for k in ["LEFT", "RIGHT", "FULL"]
                )
                if join_is_outer:
                    return super().join_sql(expression)

                # Extract all non-equality conditions (including those with literals)
                # YDB only allows equality predicates in INNER/CROSS JOIN ON
                literal_conditions: list[Expression] = []
                non_equality_conditions: list[Expression] = []
                equality_conditions: list[Expression] = []

                if isinstance(on_condition, exp.And):
                    conditions = list(on_condition.flatten())
                else:
                    conditions = [on_condition]

                for cond in conditions:
                    # Check if it's an equality predicate
                    if isinstance(cond, exp.EQ):
                        # Check if it's a true equi-join (columns from different tables)
                        left = cond.this
                        right = cond.expression
                        left_table = getattr(left, "table", None) if isinstance(left, exp.Column) else None
                        right_table = getattr(right, "table", None) if isinstance(right, exp.Column) else None
                        if (
                                isinstance(left, exp.Column)
                                and isinstance(right, exp.Column)
                                # At least one side must be table-qualified and they must differ
                                # (this covers cases where one side has no qualifier, e.g.
                                #  ps_suppkey = _u_1.s_suppkey from NOT IN unnesting)
                                and (left_table or right_table)
                                and left_table != right_table
                        ):
                            equality_conditions.append(cond)
                        else:
                            if self._contains_literals(cond):
                                literal_conditions.append(cond)
                            else:
                                non_equality_conditions.append(cond)
                    else:
                        if self._contains_literals(cond):
                            literal_conditions.append(cond)
                        else:
                            non_equality_conditions.append(cond)

                conditions_to_move = literal_conditions + non_equality_conditions

                if equality_conditions:
                    if len(equality_conditions) == 1:
                        on_condition = equality_conditions[0]
                    else:
                        on_condition = equality_conditions[0]
                        for cond in equality_conditions[1:]:
                            on_condition = exp.and_(on_condition, cond)
                    expression.set("on", on_condition)
                else:
                    # No valid equality conditions remain on the JOIN — fall back
                    # to an explicit CROSS JOIN regardless of the original kind.
                    expression.set("kind", "CROSS")
                    expression.set("on", None)
                    expression.set("side", None)

                if conditions_to_move:
                    select_stmt = expression.find_ancestor(exp.Select)
                    if select_stmt:
                        combined_condition = conditions_to_move[0]
                        for cond in conditions_to_move[1:]:
                            combined_condition = exp.and_(combined_condition, cond)

                        existing_where = select_stmt.args.get("where")
                        if existing_where:
                            new_where = exp.and_(existing_where.this, combined_condition)
                            select_stmt.set("where", exp.Where(this=new_where))
                        else:
                            select_stmt.set("where", exp.Where(this=combined_condition))

                join_sql = super().join_sql(expression)
                return join_sql

            return super().join_sql(expression)

        def update_sql(self, expression: exp.Update) -> str:
            table = expression.args.get("this")
            alias_node = table.args.get("alias") if table else None

            if alias_node:
                alias_name = alias_node.name
                expression = expression.copy()
                table = expression.args["this"]
                table.set("alias", None)

                # Strip the alias qualifier from column references in the top-level
                # SET and WHERE — but not inside subqueries (depth > 0).
                for node in expression.walk():
                    if isinstance(node, exp.Column):
                        tbl = node.args.get("table")
                        if tbl and tbl.name == alias_name:
                            p, depth = node.parent, 0
                            while p:
                                if isinstance(p, (exp.Subquery, exp.Select)):
                                    depth += 1
                                p = p.parent
                            if depth == 0:
                                node.set("table", None)

            return super().update_sql(expression)

        def select_sql(self, expression: exp.Select) -> str:
            # Store the original-to-alias mapping for GROUP BY/ORDER BY reference
            self.expression_to_alias = {}
            # Reverse mapping: alias name -> original expression (for GROUP BY expansion)
            self.alias_to_expression: dict[str, exp.Expression] = {}

            # Build mapping of original expressions to their aliases
            # After that, in WHERE and ORDER BY use aliases
            for select_expr in expression.expressions:
                if isinstance(select_expr, exp.Alias):
                    expr_sql = self.sql(select_expr.this).strip()
                    alias_name = select_expr.alias_or_name
                    self.expression_to_alias[expr_sql] = alias_name
                    self.alias_to_expression[alias_name] = select_expr.this
                else:
                    expr_sql = self.sql(select_expr).strip()
                    if isinstance(select_expr, (exp.Column, exp.Identifier)):
                        self.expression_to_alias[expr_sql] = select_expr.alias_or_name
            # in .sql() calls ww generated ydb_variables, drop it not to produce unused vars
            self.ydb_variables = {}
            return super().select_sql(expression)

        def _contains_literals(self, condition: exp.Expression) -> bool:
            return condition.find(exp.Literal) is not None

        def where_sql(self, expression: exp.Where) -> str:
            original_where = super().where_sql(expression) if expression else ""
            return original_where

        def _date_add(self, expression: exp.Expression) -> str:
            this = expression.this
            unit = unit_to_var(expression.expression)
            op = (
                "+"
                if isinstance(
                    expression, (exp.DateAdd, exp.TimeAdd, exp.DatetimeAdd, exp.TsOrDsAdd)
                )
                else "-"
            )

            expr = expression.expression

            source = None
            if isinstance(this, exp.Literal):
                if " " in this.name:
                    source = f"DateTime::MakeDateTime(DateTime::ParseIso8601({self.sql(this).replace(' ', 'T')}))"
                else:
                    source = f"CAST({self.sql(this)} AS DATE)"
            else:
                source = self.sql(this)
            if not unit:
                return ""
            if unit.name in ["MONTH", "YEARS"]:
                to_type = (
                    "DateTime"
                    if isinstance(expression, (exp.DatetimeAdd, exp.DatetimeSub))
                    else "Date"
                )
                if unit.name == "YEARS":
                    return f"DateTime::Make{to_type}(DateTime::ShiftYears({source}, {op if op == '-' else ''}{expr.name}))"
                if unit.name == "MONTH":
                    return f"DateTime::Make{to_type}(DateTime::ShiftMonths({source}, {op if op == '-' else ''}{expr.name}))"
                return ""
            else:
                if unit.name == "DAY":
                    interval_expr = f"DateTime::IntervalFromDays({expr.name})"
                elif unit.name == "HOUR":
                    interval_expr = f"DateTime::IntervalFromHours({expr.name})"
                elif unit.name == "MINUTE":
                    interval_expr = f"DateTime::IntervalFromMinutes({expr.name})"
                elif unit.name == "SECOND":
                    interval_expr = f"DateTime::IntervalFromSeconds({expr.name})"
                else:
                    raise ValueError(f"Unsupported interval type: {unit.name}")

                return f"{source} {op} {interval_expr}"

        def add_sql(self, expression: exp.Add) -> str:
            """
            Intercept date + INTERVAL n YEAR/MONTH before the default Add handler.
            YDB has no native YEAR/MONTH interval; use DateTime::ShiftYears/ShiftMonths.
            """
            return self._maybe_shift_date(expression, op="+") or super().add_sql(expression)

        def sub_sql(self, expression: exp.Sub) -> str:
            """
            Intercept date - INTERVAL n YEAR/MONTH (same as add_sql but subtraction).
            """
            return self._maybe_shift_date(expression, op="-") or super().sub_sql(expression)

        def _maybe_shift_date(self, expression: exp.Expression, op: str) -> str:
            """
            If expression is (date_expr ± INTERVAL n YEAR/MONTH), rewrite as
            DateTime::MakeDate(DateTime::ShiftYears/ShiftMonths(date_expr, ±n)).
            Returns empty string when the pattern does not match.
            """
            left = expression.this
            right = expression.expression if hasattr(expression, "expression") else expression.right
            if not isinstance(right, exp.Interval):
                return ""
            unit = right.text("unit").upper()
            if unit not in ("YEAR", "YEARS", "MONTH"):
                return ""
            value = right.text("this").strip("'")
            source = self.sql(left)
            n = f"-{value}" if op == "-" else value
            fn = "ShiftYears" if unit in ("YEAR", "YEARS") else "ShiftMonths"
            return f"DateTime::MakeDate(DateTime::{fn}({source}, {n}))"

        def interval_sql(self, expression: exp.Interval) -> str:
            """
            Convert standard SQL INTERVAL literals to YQL DateTime module calls.
            e.g. INTERVAL '30' DAY -> DateTime::IntervalFromDays(30)
            """
            unit = expression.text("unit").upper()
            value = self.sql(expression, "this")
            # Strip surrounding quotes from literal values
            if value.startswith("'") and value.endswith("'"):
                value = value[1:-1]

            mapping = {
                "DAY": f"DateTime::IntervalFromDays({value})",
                "HOUR": f"DateTime::IntervalFromHours({value})",
                "MINUTE": f"DateTime::IntervalFromMinutes({value})",
                "SECOND": f"DateTime::IntervalFromSeconds({value})",
            }
            if unit in mapping:
                return mapping[unit]
            # MONTH/YEAR intervals have no direct DateTime:: equivalent;
            # leave them as-is and let the user handle them.
            return super().interval_sql(expression)

        def _date_diff(self, expression: exp.DateDiff) -> str:
            """
            Convert dateDiff(unit, start, end) to YQL arithmetic.
            YDB Timestamps are stored in microseconds, so we cast the subtraction to Int64.
            dateDiff args: this=end, expression=start, unit=unit
            """
            unit = expression.text("unit").upper()
            end = self.sql(expression, "this")
            start = self.sql(expression, "expression")

            factors = {
                "SECOND": 1_000_000,
                "MINUTE": 60_000_000,
                "HOUR": 3_600_000_000,
                "DAY": 86_400_000_000,
                "WEEK": 604_800_000_000,
            }
            if unit in factors:
                factor = factors[unit]
                return f"(CAST({end} AS Int64) - CAST({start} AS Int64)) / {factor}"
            self.unsupported(f"DateDiff unit not supported: {unit}")
            return f"(CAST({end} AS Int64) - CAST({start} AS Int64))"

        def _arrayany(self, expression: exp.ArrayAny) -> str:
            """
            Generate SQL for ARRAY_ANY function with YDB-specific implementation.

            Args:
                expression: The ARRAY_ANY expression

            Returns:
                YDB-specific SQL for array existence checks
            """
            param = expression.expression.expressions[0]
            column_references = {}

            for ident in expression.expression.this.find_all(exp.Column):
                if len(ident.parts) < 2:
                    continue

                table_reference = ident.parts[0]
                column_reference = ident.parts[1]
                column_references[
                    f"{table_reference.alias_or_name}.{column_reference.alias_or_name}"
                ] = (table_reference, column_reference)

            if len(column_references) > 0:
                table_aliases = {}
                next_alias = name_sequence("p_")
                for column_reference in column_references:
                    table_aliases[column_reference] = next_alias()

                params_l = [
                    f"${param}" for param in [param.alias_or_name] + list(table_aliases.values())
                ]
                params = f"({', '.join(params_l)})"

                for ident in list(expression.expression.this.find_all(exp.Column)):
                    if len(ident.parts) < 2:
                        continue

                    table_reference = ident.parts[0]
                    column_reference = ident.parts[1]
                    full_column_reference = (
                        f"{table_reference.alias_or_name}.{column_reference.alias_or_name}"
                    )
                    table_alias = table_aliases[full_column_reference]
                    table_reference.pop()
                    column_reference.replace(exp.to_identifier(table_alias))

                lambda_sql = self.sql(expression.expression)
                table_aliases_sql = (
                    f"({', '.join([expression.this.alias_or_name] + list(table_aliases.keys()))})"
                )

                return f"ListHasItems({params}->(ListFilter(${param.alias_or_name}, {lambda_sql})){table_aliases_sql})"
            else:
                return f"ListHasItems(ListFilter({self.sql(expression.expression)}))"

        def _set_sql(self, expression: exp.Set) -> str:
            eq = expression.find(exp.EQ)
            if not eq:
                return ""
            var_name = exp.Identifier(this="$" + eq.this.name)

            new_eq = exp.EQ(this=var_name, expression=eq.expression)

            return self.binary(new_eq, "=")

        def _group_by(self, expression: exp.Group) -> str:
            """Generate GROUP BY using alias references."""
            select_stmt = expression.find_ancestor(exp.Select)

            if not select_stmt:
                group_by_items = ", ".join(self.sql(e) for e in expression.expressions)
                return f" GROUP BY {group_by_items}" if group_by_items else " GROUP BY"

            # If the SELECT's FROM is a subquery, the alias columns are already materialised
            # there — do NOT expand alias references in GROUP BY.
            from_node = select_stmt.args.get("from_")
            from_is_subquery = (
                from_node is not None
                and isinstance(from_node.this, exp.Subquery)
            )

            transformed = []
            for gb_expr in expression.expressions:
                gb_sql = self.sql(gb_expr).strip()

                # If this GROUP BY item is an alias of a complex SELECT expression, expand it
                # (only when FROM is NOT a subquery — otherwise the alias is a real column;
                # only expand non-trivial expressions — column aliases are handled below)
                alias_map = getattr(self, "alias_to_expression", {})
                if (
                    not from_is_subquery
                    and isinstance(gb_expr, exp.Column)
                    and gb_sql in alias_map
                    and not isinstance(alias_map[gb_sql], (exp.Column, exp.Identifier))
                ):
                    # Expand alias → full expression so YDB doesn't confuse it with a column
                    transformed.append(alias_map[gb_sql].copy())
                elif isinstance(gb_expr, (exp.Column, exp.Identifier)):
                    # Add column AS alias so YDB resolves unambiguously.
                    # Strip any table qualifier from the column (e.g. y.a -> a).
                    # Use the SELECT-level alias if the column is aliased there
                    # (e.g. `a_id AS _u_1` in SELECT means GROUP BY `a_id AS _u_1`).
                    column_name = gb_expr.alias_or_name
                    expr_to_alias = getattr(self, "expression_to_alias", {})
                    alias_name = expr_to_alias.get(column_name, column_name)
                    unqualified_col = exp.column(column_name)
                    transformed.append(exp.alias_(unqualified_col, alias_name))
                else:
                    transformed.append(gb_expr)

            group_by_items = ", ".join(f"{self.sql(e)}" for e in transformed) if transformed else ""

            # Handle ROLLUP, CUBE, and GROUPING SETS
            rollup = self.expressions(expression, key="rollup")
            cube = self.expressions(expression, key="cube")
            grouping_sets = self.expressions(expression, key="grouping_sets")

            # Build the GROUP BY clause
            if group_by_items:
                result = f" GROUP BY {group_by_items}"
            elif not (rollup or cube or grouping_sets):
                return ""
            else:
                result = " GROUP BY"

            # Add ROLLUP, CUBE, or GROUPING SETS
            if rollup:
                result += f" {rollup}"
            elif cube:
                result += f" {cube}"
            elif grouping_sets:
                result += f" {grouping_sets}"

            return result

        # YDB uses C-like string escaping: backslash must be doubled in literals.
        _YDB_ESCAPE_MAP = str.maketrans({
            "\x07": "\\a", "\x08": "\\b", "\x0c": "\\f",
            "\n": "\\n", "\r": "\\r", "\t": "\\t", "\x0b": "\\v",
            "\\": "\\\\",
        })

        def escape_str(self, text: str, escape_backslash: bool = True, **kwargs) -> str:
            if escape_backslash:
                text = text.translate(self._YDB_ESCAPE_MAP)
            # Escape the single-quote delimiter the normal way ('' or \')
            return text.replace("'", "''")

        def not_sql(self, expression: exp.Not) -> str:
            """YDB requires explicit parentheses around LIKE inside NOT."""
            inner = expression.this
            if isinstance(inner, (exp.Like, exp.ILike, exp.SimilarTo)):
                return f"NOT ({self.sql(inner)})"
            return super().not_sql(expression)

        def ordered_sql(self, expression: exp.Ordered) -> str:
            """YDB does not support NULLS FIRST / NULLS LAST — strip them."""
            expression = expression.copy()
            expression.set("nulls_first", None)
            return super().ordered_sql(expression)

        def _order_sql(self, expression: exp.Order) -> str:
            """Generate ORDER BY using alias references."""
            select_stmt = expression.find_ancestor(exp.Select)

            if not select_stmt:
                return super().order_sql(expression)

            orders = []
            for order_expr in expression.expressions:
                if isinstance(order_expr, exp.Ordered):
                    expr = order_expr.this
                    expr_sql = self.sql(expr).strip()

                    if (
                            hasattr(self, "expression_to_alias")
                            and expr_sql in self.expression_to_alias
                    ):
                        alias_name = self.expression_to_alias[expr_sql]
                        alias_expr = exp.to_identifier(alias_name)
                        ordered = exp.Ordered(this=alias_expr, desc=order_expr.args.get("desc"))
                        orders.append(ordered)
                    else:
                        orders.append(order_expr)
                else:
                    expr_sql = self.sql(order_expr).strip()
                    if (
                            hasattr(self, "expression_to_alias")
                            and expr_sql in self.expression_to_alias
                    ):
                        alias_name = self.expression_to_alias[expr_sql]
                        alias_expr = exp.to_identifier(alias_name)
                        orders.append(alias_expr)
                    else:
                        orders.append(order_expr)
            if not orders:
                return ""

            order_sql = ", ".join(self.sql(e) for e in orders)
            return f" ORDER BY {order_sql}"

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            **STRING_TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "Int8",
            exp.DataType.Type.SMALLINT: "Int16",
            exp.DataType.Type.INT: "Int32",
            exp.DataType.Type.BIGINT: "Int64",
            exp.DataType.Type.DECIMAL: "Decimal",
            exp.DataType.Type.FLOAT: "Float",
            exp.DataType.Type.DOUBLE: "Double",
            exp.DataType.Type.BOOLEAN: "Uint8",
            exp.DataType.Type.TIMESTAMP: "Timestamp",
            exp.DataType.Type.BIT: "Uint8",
            exp.DataType.Type.VARCHAR: "String",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            FlattenBy: lambda self, e: self.flattenby_sql(e),
            AssumeOrderBy: lambda self, e: self.assumeorderby_sql(e),
            YdbTuple: lambda self, e: self.ydbtuple_sql(e),
            YdbAtString: lambda self, e: self.ydbatstring_sql(e),
            YdbPostfixCall: lambda self, e: self.ydbpostfixcall_sql(e),
            YdbJsonExists: lambda self, e: self.ydbjsonexists_sql(e),
            YdbJsonQuery: lambda self, e: self.ydbjsonquery_sql(e),
            YdbJsonValue: lambda self, e: self.ydbjsonvalue_sql(e),
            YdbJsonPassingItem: lambda self, e: self.ydbjsonpassingitem_sql(e),
            YdbLambdaBlock: lambda self, e: self.ydblambdablock_sql(e),
            exp.Create: create_sql,
            exp.DefaultColumnConstraint: lambda self, e: "",
            exp.DateTrunc: _date_trunc_sql,
            exp.Select: transforms.preprocess(
                [apply_alias_to_select_from_table, move_ctes_to_top_level]
            ),
            exp.CurrentTimestamp: _current_timestamp_sql,
            exp.StrToDate: _str_to_date,
            exp.Extract: _extract,
            exp.ArraySize: rename_func_not_normalize("ListLength"),
            exp.ArrayFilter: rename_func_not_normalize("ListFilter"),
            exp.Lambda: _lambda,
            exp.ArrayAny: _arrayany,
            exp.ArrayAgg: rename_func_not_normalize("AGGREGATE_LIST"),
            exp.Concat: concat_to_dpipe_sql,
            exp.If: _if,
            exp.Nullif: _null_if,
            exp.DateAdd: _date_add,
            exp.DateSub: _date_add,
            exp.DateDiff: _date_diff,
            exp.JSONBContains: rename_func_not_normalize("Yson::Contains"),
            exp.ForeignKey: lambda self, e: self.unsupported("constraint not supported"),
            exp.StringToArray: rename_func_not_normalize("String::SplitToList"),
            exp.Array: rename_func_not_normalize("AsList"),
            exp.ArrayToString: rename_func_not_normalize("String::JoinFromList"),
            exp.Upper: rename_func_not_normalize("Unicode::ToUpper"),
            exp.Lower: rename_func_not_normalize("Unicode::ToLower"),
            exp.StrPosition: rename_func_not_normalize("Find"),
            exp.Length: rename_func_not_normalize("Unicode::GetLength"),
            exp.Unnest: rename_func_not_normalize("FLATTEN BY"),
            # exp.Round handled by round_sql (precision sign must be negated)
            exp.Set: _set_sql,
            exp.Group: _group_by,
            exp.Order: _order_sql,
            exp.RegexpReplace: lambda self, e: (
                f"Re2::Replace({self.sql(e, 'expression')})"
                f"({self.sql(e, 'this')}, {self.sql(e, 'replacement')})"
            ),
        }
