/**
 * Extracts the parameter names of a URI template, which are the names between curly braces.
 * @param templateStr A URI template of a derived resource, e.g. `http://example.org/data/{p1}`.
 * @returns The names of all parameters occurring in the template.
 */
export function extractTemplateParams(templateStr: string): Set<string> {
  const matches = templateStr.matchAll(/\{([^}]+)\}/g);
  return new Set(Array.from(matches, m => m[1]));
}

/**
 * Takes the parameters from the template (in { } brackets) and replaces those
 * with temporary parameters
 * @param rawQuery The query of a derived resource, with parameters between dollar signs.
 * @param paramNames The parameter names to replace.
 * @returns The query with all parameters replaced by variables, and those variable names.
 */
export function normalizeQuery(rawQuery: string, paramNames: Set<string>): {
  normalized: string;
  newVariables: Set<unknown>;
} {
  let normalized = rawQuery;

  const newVariables = new Set();
  for (const param of paramNames) {
    // Escapes and replaces $param$ with a distinct valid variable
    const regex = new RegExp(`\\$${param}\\$`, 'g');
    normalized = normalized.replace(regex, `?__param_${param}`);
    newVariables.add(`?__param_${param}`);
  }
  return {
    normalized,
    newVariables,
  };
}
