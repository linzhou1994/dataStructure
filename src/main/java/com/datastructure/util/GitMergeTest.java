package com.datastructure.util;

public class GitMergeTest {

    private Integer gitAddress;
    private String message;

    public GitMergeTest(String message) {
        this.message = message;
    }

    public GitMergeTest(Integer gitAddress) {
        this.gitAddress = gitAddress;
    }

    public Integer getGitAddress() {
        return gitAddress;
    }

    public void setGitAddress(Integer gitAddress) {
        this.gitAddress = gitAddress;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
