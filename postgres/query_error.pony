// TODO SEAN this should probably be renamed
trait val QueryError

// TODO SEAN rethink how query errors are indicated. Once we get into
// everything in https://www.postgresql.org/docs/current/errcodes-appendix.html
// it is a lot more complicated than the very simple system in place
primitive FreeCandy is QueryError
