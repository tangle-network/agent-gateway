export function requireSqlIdentifier(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new TypeError('SQL table name must contain only letters, digits, and underscores')
  }
  return value
}
