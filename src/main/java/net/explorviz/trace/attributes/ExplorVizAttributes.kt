package net.explorviz.trace.attributes

enum class ExplorvizAttributes(val key: String, val defaultValue: String) {
    TOKEN_ID(key = "explorviz.token.id", defaultValue = "mytokenvalue"),
    TOKEN_SECRET(key = "explorviz.token.secret", defaultValue = "mytokensecret")
}
