package net.explorviz.trace.adapter.service.parsing.fqn

interface FqnParser {
    data class FqnParseResult(val filePath: String, val functionName: String, val className: String? = null)

    /**
     * Parses a function fqn according to some language specification and separates into:
     * * the associated file's relative path as given from the fqn (separated by "/"). The file path should crucially
     *   _include a file extension_ if applicable.
     * * the (unqualified) name of the function
     * * the name of a class within the file, if included in the fqn, otherwise null. The name of any inner class is
     *   qualified against its parent classes (separated by ".").
     *
     * See [OTel semantic conventions](https://opentelemetry.io/docs/specs/semconv/registry/attributes/code/) for
     * examples on how the fqn is structured for different languages.
     */
    fun parseFunctionFqn(functionFqn: String): FqnParseResult
}
