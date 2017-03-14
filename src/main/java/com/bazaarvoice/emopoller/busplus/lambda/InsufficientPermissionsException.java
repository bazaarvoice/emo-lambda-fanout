package com.bazaarvoice.emopoller.busplus.lambda;

import com.google.common.collect.ImmutableSet;

public class InsufficientPermissionsException extends Exception {
    private final String principal;
    private final ImmutableSet<String> permissions;

    InsufficientPermissionsException(final String principal, final ImmutableSet<String> permissions){
        super(String.format("Principal[%s] requires permissions[%s] but lacks them.", principal, permissions));
        this.principal = principal;
        this.permissions = permissions;
    }

    public String getPrincipal() {
        return principal;
    }

    public ImmutableSet<String> getPermissions() {
        return permissions;
    }
}
