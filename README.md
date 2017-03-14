This application features a prototype endpoint that invokes lambda functions on databus events.

It only accepts pre-approved keys (all others will get a 401 Unauthorized).
These keys must be hashed and stored in the config file.

API
---

### GET     `/poller`

List all function subscriptions for your API key.


### GET     `/poller?lambdaArn={arn}`

Get a specific function subscription by Lambda ARN.

### GET     `/poller/size?lambdaArn={arn}`

Get the number of queued events for a function, specified by Lambda ARN.

### POST    `/poller?lambdaArn={lambdaArn}&claimTtl={claimTtl}[&batchSize={batchSize}]  body:{condition}`

Create a lambda subscription.
* `{lambdaArn}`:    The ARN of the lambda function to invoke.
* `{claimTtl}`:     How long before the event will be handled again. This should be longer than you expect the function to ever take.
* `[{batchSize}]`:     Optional. How many messages to handle with each function invocation. For example, the minimum execution time for a lambda function is 100ms. If the actual run time of the function is 30ms, you can set `batchSize` to 3 and effectively get 3 executions for the price of one! 
* **request body**:   An EmoDB subscription condition.

See https://bazaarvoice.github.io/emodb/databus/ "Subscription Management" for more info.

### DELETE     `/poller?lambdaArn={arn}`

Deactivates and un-registers the function, specified by Lambda ARN. Note that this does not immediately clear the databus, so deleting and recreating the function will cause it to pick up where it left off.

### POST       `/poller/activate?lambdaArn={arn}`

Re-activates an inactive function, specified by Lambda ARN. This works whether the function got deactivated due to a `DELETE` request or 
too many consecutive function errors.


Security Documentation
======================

Key Handling
------------

### Api key whitelist

This application will only accept emodb api keys that have been explicitly whitelisted.

The whitelist is stored in the application config (e.g., [the production config](src/main/config/config.bazaar.yaml)) under `poller.apiKeyDigestWhitelist`.

The whitelist contains argon2 hashes of the api key.

To create a hash, run https://github.com/bazaarvoice/emo-lambda-fanout/blob/master/deploy/hash-key.sh thusly:

    bin/hash-key.sh MyPlainTextKey

The actual code that generates the digest is here: https://github.com/bazaarvoice/emo-lambda-fanout/blob/master/src/main/java/com/bazaarvoice/emopoller/tools/HashKeyCommand.java.

All API requests are handled in https://github.com/bazaarvoice/emo-lambda-fanout/blob/master/src/main/java/com/bazaarvoice/emopoller/resource/PollerResource.java, which uses `getKey()` to extract and validate the api key header (https://github.com/bazaarvoice/emo-lambda-fanout/blob/master/src/main/java/com/bazaarvoice/emopoller/resource/PollerResource.java#L237).

`getKey()` requires the presence of the api key in the headers, computes the base64-encoded sha512 hash of the request key and requires that the computed digest is present in the whitelist loaded from config.

Beliefs:
* We believe this is sufficient to ensure that no key that has not been explicitly whitelisted will be able to perform any API actions. 
* And additionally we believe that the whitelist is itself safe to store in a public location, as it should not be possible to derive any useful information from the hash.

### Application API key handling

This application requires an EmoDB api key for managing its own application state.

This key is stored (encrypted) in the config file under `emodb.apiKey` (https://github.com/bazaarvoice/emo-lambda-fanout/blob/master/src/main/config/config.bazaar.yaml#L10`).

For encryption, we use AWS KMS.

To encrypt the application key, we use https://github.com/bazaarvoice/emo-lambda-fanout/blob/master/deploy/kms/encrypt-app-key.sh. The result of this is what we store in the config file.

Upon application startup, we invoke https://github.com/bazaarvoice/emo-lambda-fanout/blob/master/src/main/java/com/bazaarvoice/emopoller/busplus/LambdaSubscriptionManager.java#L94, which calls https://github.com/bazaarvoice/emo-lambda-fanout/blob/master/src/main/java/com/bazaarvoice/emopoller/busplus/kms/ApiKeyCrypto.java#L64, which in turn invokes KMS to decrypt the key.

The way that KMS works, anyone who has Decrypt permission for our Customer Master Key (CMK) will be able to use KMS to decrypt the application key.

So the application needs permission to decrypt the api key by virtue of the role assigned to its instance profile.

### Delegate API key handling

This application provides an additional feature of being able to register an AWS Lambda function to an EmoDB subscription. The way this works is that you create a lambda function and a policy that can invoke the function. Currently, an administrator (me) has to add that policy to the app's role. 

Then, you can call https://github.com/bazaarvoice/emo-lambda-fanout/blob/master/src/main/java/com/bazaarvoice/emopoller/resource/PollerResource.java#L150 and to hook the Lambda function up to a subscription.

Since the poller will execute EmoDB `poll` operations on your behalf, it has to store the api key you provide. This storage mechanism is similar to the application API key handling procedure.

We encrypt the key using our CMK here: https://github.com/bazaarvoice/emo-lambda-fanout/blob/master/src/main/java/com/bazaarvoice/emopoller/busplus/LambdaSubscriptionManager.java#L194.

Then, when the time comes to poll the subscription, we decrypt it here: https://github.com/bazaarvoice/emo-lambda-fanout/blob/master/src/main/java/com/bazaarvoice/emopoller/busplus/LambdaSubscriptionManager.java#L315.


### Beliefs about API key handling

* Both the lambda-fanout application's key and the subscription keys are stored encrypted in public locations (the config file and in an EmoDB table). We believe it is safe to do so because:
** Only the key administrator (jroesler) and the application role will be able to decrypt the key.
** No outsider will be able to assume the role that can decrypt the key.
** No outsider will be able to gain access to the lambda-fanout instances (any decrypt request originating from the machine will be able to decrypt the key)

Development
-----------

IDEA
====

```
Main class:               com.bazaarvoice.emopoller.emopollerApp
Program arguments:        server src/main/config/config.local.yaml
Working directory:        /home/YOU/repos/baz/emo-lambda-fanout (or wherever you keep it)
User classpath of module: emo-lambda-fanout
JRE:                      1.8
```

Adding support for a new Emo API key
====================================

The app will refuse to work with keys that are not whitelisted. This is to control who
gets to use our service.

Emo keys are secret!!! There's no way around handling them in plain text when we want
to support a new key, but you *must exercise caution* not to leak the key with a careless
chat message, email, code checkin, etc., etc. 

1. Have the key's owner send it to you over an encrypted channel, such as Box.
2. Hash the key thusly: `bin/hash-key.sh MyKeyGoesHere`.
    * this gives you something like: `JkKamLdE4pR+g1KgaM+iC6e0YmvG8+CjxsOGvRAlxeF1MvlFTMyrwpvgDmjPuaDLzqRHghp/WNjGetioS1foDA==`.
3. Add it to `poller.apiKeyDigestWhitelist` in `src/main/config/config/config.bazaar.yaml`
4. Follow the release/deploy instructions to deploy a new generation of proxies that accept this key.
