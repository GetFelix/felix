A token every broker refuses at connect is now an auth error rather than "no broker answered", and the connect timeout no longer includes the token provider's fetch, which has its own 30 s limit.
